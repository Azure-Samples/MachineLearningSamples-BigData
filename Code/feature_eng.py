import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, col, udf, lag, date_add, explode, lit, unix_timestamp
from pyspark.sql.functions import year, month, weekofyear, dayofmonth, hour, date_format 
from pyspark.sql.types import *
from pyspark.sql.types import DateType
from pyspark.sql.dataframe import *
from pyspark.sql.window import Window
import datetime
from pandas.tseries.holiday import USFederalHolidayCalendar
#####################################################
# Create the time series for per-hour  
# UDF
#####################################################
def generate_hour_series(start, stop, window=3600):
    begin = start - start%window
    end =   stop - stop%window + window
    return [begin + x for x in range(0, end-begin + 1, window)] 

def generate_day_series(start, stop, window=3600*24):
    begin = start - start%window
    end =   stop - stop%window + window
    return [begin + x for x in range(0, end-begin + 1, window)] 

def getAllHours(spark, trainBegin, trainEnd):
    # Register UDF 
    spark.udf.register("generate_hour_series", generate_hour_series, ArrayType(IntegerType()) )
    sqlStatement = """ SELECT explode(   generate_hour_series( UNIX_TIMESTAMP('{0!s}', "yyyy-MM-dd HH:mm:ss"),
             UNIX_TIMESTAMP('{1!s}', 'yyyy-MM-dd HH:mm:ss')) )
       """.format(trainBegin, trainEnd)
    timeDf = spark.sql(sqlStatement)
    timeDf=timeDf.withColumn("Time", col('col').cast(TimestampType()))
    timeDf = timeDf.select(col("Time"))
    return timeDf

def getAllDays(spark, trainBegin, trainEnd):
    # Register UDF 
    spark.udf.register("generate_day_series", generate_hour_series, ArrayType(IntegerType()) )
    sqlStatement = """ SELECT explode(   generate_day_series( UNIX_TIMESTAMP('{0!s}', "yyyy-MM-dd HH:mm:ss"),
             UNIX_TIMESTAMP('{1!s}', 'yyyy-MM-dd HH:mm:ss')) )
       """.format(trainBegin, trainEnd)
    timeDf = spark.sql(sqlStatement)
    timeDf=timeDf.withColumn("Time", col('col').cast(TimestampType()))
    timeDf = timeDf.select(col("Time"))
    return timeDf
    
def createDailyBuckets(dailyDf, timeDailyDf):
    IPDf = dailyDf.select(col("d_ServerIP")).distinct().withColumnRenamed('d_ServerIP','ServerIP')
    bucketDf = timeDailyDf.crossJoin(IPDf)
    bucketDf = bucketDf.withColumn("d_key2", concat(bucketDf.ServerIP,lit("_"),bucketDf.Time.cast('string')))
    #print(bucketDf.count())
    return bucketDf

def createHourlyBuckets(dailyDf, timeHourlyDf):
    IPDf = dailyDf.select(col("d_ServerIP")).distinct().withColumnRenamed('d_ServerIP','ServerIP')
    bucketDf = timeHourlyDf.crossJoin(IPDf)
    #print(timeDf.count())
    bucketDf = bucketDf.withColumn("h_key2", concat(bucketDf.ServerIP,lit("_"),bucketDf.Time.cast('string')))
    #print(bucketDf.count())
    return bucketDf
def addLagForDailyFeature(featureDf):
    #lag features
    #previous week average
    #rolling mean features with 2-days/48-hours lag
    rollingLags = [2]
    lagColumns = [x for x in featureDf.columns if 'Daily' in x] 
    windowSize=[7]
    for w in windowSize:
        for i in rollingLags:
            wSpec = Window.partitionBy('ServerIP').orderBy('Time').rowsBetween(-i-w, -i-1)
            for j in lagColumns:
                featureDf = featureDf.withColumn(j+'Lag'+str(i)+'Win'+str(w),F.avg(col(j)).over(wSpec) )
    return featureDf

def addLagForHourlyFeature(featureDf):
    # lag features
    previousWeek=int(24*7)
    previousMonth=int(24*365.25/12)
    lags=[48, 49, 50, 51, 52, 55, 60, 67, 72, 96]
    lags.extend([previousWeek, previousMonth])
    lagColumns = ['peakLoad']
    for i in lags:
        wSpec = Window.partitionBy('ServerIP').orderBy('Time')
        for j in lagColumns:
            featureDf = featureDf.withColumn(j+'Lag'+str(i),lag(featureDf[j], i).over(wSpec) )
    return featureDf

def addTimeFeature(featureDf, trainBegin):
    # Extract some time features from "Time" column
    featureDf = featureDf.withColumn('year', year(featureDf['Time']))
    featureDf = featureDf.withColumn('month', month(featureDf['Time']))
    featureDf = featureDf.withColumn('weekofyear', weekofyear(featureDf['Time']))
    featureDf = featureDf.withColumn('dayofmonth', dayofmonth(featureDf['Time']))
    featureDf = featureDf.withColumn('hourofday', hour(featureDf['Time']))
    dayofweek = F.date_format(featureDf['Time'], 'EEEE')

    featureDf = featureDf.withColumn('dayofweek', dayofweek )

    featureDf = featureDf.select([x for x in featureDf.columns if 'd_' not in x ])

    ################################
    trainBeginTimestamp = int(datetime.datetime.strftime(  datetime.datetime.strptime(trainBegin, "%Y-%m-%d %H:%M:%S") ,"%s"))
    def linearTrend(x):
        if x is None:
            return 0
        # return # of hour since the beginning
        return (x-trainBeginTimestamp)/3600/24/365.25
 
    linearTrendUdf =  udf(linearTrend,IntegerType())
    featureDf = featureDf.withColumn('linearTrend',linearTrendUdf(F.unix_timestamp('Time')))


    # use the following two as the range to  calculate the holidays in the range of  [holidaBegin, holidayEnd ]
    holidayBegin = '2009-01-01'
    holidayEnd='2016-06-30'
    cal = USFederalHolidayCalendar()
    holidays_datetime = cal.holidays(start=holidayBegin, end=holidayEnd).to_pydatetime()
    holidays = [t.strftime("%Y-%m-%d") for t in holidays_datetime]


    def isHoliday(x):
        if x is None:
            return 0
        if x in holidays:
            return 1
        else:
            return 0
    isHolidayUdf =  udf (isHoliday, IntegerType())
    featureDf= featureDf.withColumn('date', date_format(col('SessionStartHourTime'), 'yyyy-MM-dd'))
    featureDf = featureDf.withColumn("Holiday",isHolidayUdf('date'))
    

    def isBusinessHour(x):
        if x is None:
            return 0
        if x >=8 and x <=18:
            return 1
        else:
            return 0
    isBusinessHourUdf =  udf (isBusinessHour, IntegerType())
    featureDf = featureDf.withColumn("BusinessHour",isBusinessHourUdf('hourofday'))

    def isMorning(x):
        if x is None:
            return 0
        if x >=6 and x <=9:
            return 1
        else:
            return 0
    isMorningUdf =  udf (isMorning, IntegerType())
    featureDf = featureDf.withColumn("Morning",isMorningUdf('hourofday'))
    return featureDf
