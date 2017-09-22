"""
This script performs the data loading, data preparation and feature enginnering.
It takes two arguments:
1. The configuration file which contains the Azure storage account name, key and data source location. By default, it is "./Config/storageconfig.json"
2. a DEBUG argument which is a string. If set to "FILTER_IP", the filtering down two IP addresses takes effect. By default, it is "FALSE". 
"""

import os
import sys
import time
import datetime
import json
from pandas.tseries.holiday import USFederalHolidayCalendar
from util import write_blob,read_blob

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
from pyspark.sql import Row
from pyspark.ml.classification import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer
from pyspark.ml.feature import StandardScaler, PCA, RFormula
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.types import Row
from pyspark.mllib.linalg import DenseVector



from azureml.logging import get_azureml_logger

# initialize logger
run_logger = get_azureml_logger()


# load storage configuration
configFilename = "./Config/storageconfig.json"

if len(sys.argv) > 1:
    configFilename = sys.argv[1]

with open(configFilename) as configFile:    
    config = json.load(configFile)
    global storageAccount, storageContainer, storageKey, dataFile, duration
    storageAccount = config['storageAccount']
    storageContainer = config['storageContainer']
    storageKey = config['storageKey']
    dataFile = config['dataFile']
    duration = config['duration']
    print("storageContainer " + storageContainer)
    

DEBUG = 'FALSE'
#'FILTER_IP'
# 3 use a few IP 'FILTER_IP'
if len(sys.argv) > 2:
    DEBUG = sys.argv[2]
    
# 'ONE_MONTH' use a month's data 
# 'ONE_YEAR' use a year's data  
# 'ALL_YEAR' use all year's data
# 'FULL' use full dataset with duplicated data copies    
#path to save the intermediate results and models
path = "wasb://{}@{}.blob.core.windows.net/".format(storageContainer, storageAccount)

# location of the intermediate results 
mlSourceDFFile = path + 'mlSource.parquet'

# location of the models
stringIndexModelFile = path + 'stringIndexModel'
oneHotEncoderModelFile = path + 'oneHotEncoderModel'
featureScaleModelFile = path + 'featureScaleModel'
infoFile =  "info"



info = None

if duration == 'ONE_MONTH':
    trainBegin = '2016-06-01 00:00:00'
    trainEnd = '2016-06-30 23:59:59'
    testSplitStart = '2016-06-29 00:00:00'
    info = {"trainBegin":trainBegin, "trainEnd": trainEnd, "testSplitStart": testSplitStart, "dataFile": dataFile, "duration": duration}

if duration == 'FULL':
    trainBegin = '2009-01-01 00:00:00'
    trainEnd = '2016-06-30 23:59:59'
    testSplitStart = '2016-06-01 00:00:00'
    info = {"trainBegin":trainBegin, "trainEnd": trainEnd, "testSplitStart": testSplitStart, "dataFile": dataFile, "duration": duration}


# start Spark session
spark = pyspark.sql.SparkSession.builder.appName('etl').getOrCreate()

def attach_storage_container(spark, account, key):
    config = spark._sc._jsc.hadoopConfiguration()
    setting = "fs.azure.account.key." + account + ".blob.core.windows.net"
    if not config.get(setting):
        config.set(setting, key)

# attach the blob storage to the spark cluster or VM so that the storage can be accessed by the cluste or VM        
attach_storage_container(spark, storageAccount, storageKey)
# print runtime versions
print ('****************')
print ('Python version: {}'.format(sys.version))
print ('Spark version: {}'.format(spark.version))
print(spark.sparkContext.getConf().getAll())
print ('****************')




# load csv files in blob storage into Spark dataframe
# import time
print(time.time())
dataFileSep = ','
print(dataFile)
run_logger.log("reading file from ", dataFile)
df = spark.read.csv(dataFile, header=False, sep=dataFileSep, inferSchema=True, nanValue="", mode='PERMISSIVE')
print(time.time())


# rename the columns
oldnames = df.columns

newColumns=['TrafficType',"SessionStart","SessionEnd", "ConcurrentConnectionCounts", "MbytesTransferred", 
            "ServiceGrade","HTTP1","ServerType", 
            "SubService_1_Load","SubSerivce_2_Load", "SubSerivce_3_Load", 
           "SubSerivce_4_Load", "SubSerivce_5_Load", "SecureBytes_Load", "TotalLoad", 'ServerIP', 'ClientIP']
newdf = df.select([col(oldnames[index]).alias(newColumns[index]) for index in range(0,len(oldnames))])

if DEBUG == "FILTER_IP":
    IPList={'115.220.193.16','210.181.165.92'}
    filterdf = newdf.filter(newdf["ServerIP"].isin(IPList) == True)
    newdf = filterdf

# add per five minutes feature
seconds = 300
seconds_window = F.from_unixtime(F.unix_timestamp('SessionStart') - F.unix_timestamp('SessionStart') % seconds)
newdf = newdf.withColumn('SessionStartFiveMin', seconds_window.cast('timestamp'))

# aggreagte per five minutes
newdf.createOrReplaceTempView("newdf")
sqlStatement = """
    SELECT ServerIP, SessionStartFiveMin , 
    sum(TotalLoad) SumTotalLoad, count(*) NumSession,
    sum(MbytesTransferred) SumMBytes, 
    sum(SubService_1_Load) SumLoad1, sum(SubSerivce_2_Load) SumLoad2, sum(SubSerivce_3_Load) SumLoad3, 
    sum(SubSerivce_4_Load) SumLoad4, sum(SubSerivce_5_Load) SumLoad5, sum(SecureBytes_Load) SumLoadSecure
    FROM newdf group by ServerIP, SessionStartFiveMin
"""
aggregatedf = spark.sql(sqlStatement);
aggregatedf.cache()

#########################

#Create the time series for per-five minutes buckets 
# UDF
def generate_date_series(start, stop, window=300):
    begin = start - start%window
    end =   stop - stop%window + window
    return [begin + x for x in range(0, end-begin + 1, window)]  

# Register UDF for later usage
spark.udf.register("generate_date_series", generate_date_series, ArrayType(IntegerType()) )
sqlStatement = """ SELECT explode(   generate_date_series( UNIX_TIMESTAMP('{0!s}', "yyyy-MM-dd HH:mm:ss"),
             UNIX_TIMESTAMP('{1!s}', 'yyyy-MM-dd HH:mm:ss')) )
       """.format(info['trainBegin'], info['trainEnd'])
timeDf = spark.sql(sqlStatement)

timeDf=timeDf.withColumn("Time", col('col').cast(TimestampType()))

numRowsinTimeDF=timeDf.count()
run_logger.log("numRowsinTimeDF", numRowsinTimeDF)
timeDf.persist()

##########################
# join the timeDf to form dataframe with each per-five-minute bucket filled with proper data or null
joindf = timeDf.join(aggregatedf, aggregatedf.SessionStartFiveMin==timeDf.Time, "outer")

# add hour feature
secondsInHour = 3600  
hour_window = F.from_unixtime(F.unix_timestamp('SessionStartFiveMin') - F.unix_timestamp('SessionStartFiveMin') % secondsInHour)
joindf = joindf.withColumn('SessionStartHourTime', hour_window.cast('timestamp'))

#aggregatedf = aggregatedf.withColumn('SessionStartHourTime', col('SessionStartHour').cast('timestamp'))

joindf = joindf.withColumn("key", concat(joindf.ServerIP,lit("_"),joindf.SessionStartHourTime.cast('string')))
joindf.cache()

joindf = joindf.fillna(0, subset=['SumTotalLoad'])

# get the peakload every five minutes (non-overlapping) per hour
maxByGroup = (joindf.rdd
  .map(lambda x: (x[-1], x))  # Convert to PairwiseRD
  # Take maximum of the passed arguments by the last element (key)
  # equivalent to:
  # lambda x, y: x if x[-1] > y[-1] else y
  # 4 is the SumTotalLoad
  .reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[4])) 
  .values()) # Drop keys
aggregatemaxdf = maxByGroup.toDF()

featureeddf = None
aggregatemaxdf.createOrReplaceTempView("aggregatemaxdf")
sqlStatement = """
    SELECT key, ServerIP, SessionStartHourTime,
    SumTotalLoad peakLoad,
    SumMBytes peakBytes, 
    SumLoad1 peakLoad1, SumLoad2 peakLoad2, SumLoad3 peakLoad3,  
    SumLoad4 peakLoad4, SumLoad5 peakLoad5, SumLoadSecure peakLoadSecure
    FROM aggregatemaxdf 
"""

featureeddf = spark.sql(sqlStatement);
############################################
# Extract some time features from "SessionStartHourTime" column
featureeddf = featureeddf.withColumn('year', year(featureeddf['SessionStartHourTime']))
featureeddf = featureeddf.withColumn('month', month(featureeddf['SessionStartHourTime']))
featureeddf = featureeddf.withColumn('weekofyear', weekofyear(featureeddf['SessionStartHourTime']))
featureeddf = featureeddf.withColumn('dayofmonth', dayofmonth(featureeddf['SessionStartHourTime']))
featureeddf = featureeddf.withColumn('hourofday', hour(featureeddf['SessionStartHourTime']))
dayofweek = F.date_format(featureeddf['SessionStartHourTime'], 'EEEE')

featureeddf = featureeddf.withColumn('dayofweek', dayofweek )
# add day feature
day = 3600*24  
day_window = F.from_unixtime(F.unix_timestamp('SessionStartHourTime') - F.unix_timestamp('SessionStartHourTime') % day)
featureeddf = featureeddf.withColumn('SessionStartDay', day_window)
# aggreagte daily
featureeddf.createOrReplaceTempView("featureeddf")
sqlStatement = """
    SELECT ServerIP d_ServerIP, SessionStartDay d_SessionStartDay,
    AVG(peakLoad) peakLoadDaily,
    AVG(peakBytes) peakBytesDaily, 
    AVG(peakLoad1) peakLoad1Daily, AVG(peakLoad2) peakLoad2Daily, AVG(peakLoad3) peakLoad3Daily,  
    AVG(peakLoad4) peakLoad4Daily, AVG(peakLoad5) peakLoad5Daily, AVG(peakLoadSecure) peakLoadSecureDaily
    FROM featureeddf group by ServerIP, SessionStartDay
"""

dailyStatisticdf = spark.sql(sqlStatement);
#lag features
#previous week average
#rolling mean features with 2-days/48-hours lag
rollingLags = [2]
lagColumns = [x for x in dailyStatisticdf.columns if 'Daily' in x] 
windowSize=[7]
for w in windowSize:
    for i in rollingLags:
        wSpec = Window.partitionBy('d_ServerIP').orderBy('d_SessionStartDay').rowsBetween(-i-w, -i-1)
        for j in lagColumns:
            dailyStatisticdf = dailyStatisticdf.withColumn(j+'Lag'+str(i)+'Win'+str(w),F.avg(col(j)).over(wSpec) )
selectColumn =  ['d_ServerIP', 'd_SessionStartDay']
selectColumn.extend([x for x in dailyStatisticdf.columns if 'Lag' in x])
dailyStatisticdf = dailyStatisticdf.select(selectColumn)
dailyStatisticdf = dailyStatisticdf.withColumn("d_key2", concat(dailyStatisticdf.d_ServerIP,lit("_"),dailyStatisticdf.d_SessionStartDay.cast('string')))
featureeddf = featureeddf.withColumn("d_key2", concat(featureeddf.ServerIP,lit("_"),featureeddf.SessionStartDay.cast('string')))
dailyStatisticdf.cache()
# Single column join is much faster than two columns join
featureeddf = featureeddf.join(dailyStatisticdf,  (featureeddf.d_key2 == dailyStatisticdf.d_key2),
                                   'outer' )
featureeddf.show(1)
featureeddf.persist()
featureeddf = featureeddf.select([x for x in featureeddf.columns if 'd_' not in x ])

################################
trainBeginTimestamp = int(datetime.datetime.strftime(  datetime.datetime.strptime(info['trainBegin'], "%Y-%m-%d %H:%M:%S") ,"%s"))
def linearTrend(x):
    if x is None:
        return 0
    # return # of hour since the beginning
    return (x-trainBeginTimestamp)/3600/24/365.25
# 
linearTrendUdf =  udf(linearTrend,IntegerType())
featureeddf = featureeddf.withColumn('linearTrend',linearTrendUdf(F.unix_timestamp('SessionStartHourTime')))


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
featureeddf= featureeddf.withColumn('date', date_format(col('SessionStartHourTime'), 'yyyy-MM-dd'))
featureeddf = featureeddf.withColumn("Holiday",isHolidayUdf('date'))
#featureeddf.select(['date', 'Holiday'],).dropDuplicates().orderBy('date').show(20)

def isBusinessHour(x):
    if x is None:
        return 0
    if x >=8 and x <=18:
        return 1
    else:
        return 0
isBusinessHourUdf =  udf (isBusinessHour, IntegerType())
featureeddf = featureeddf.withColumn("BusinessHour",isBusinessHourUdf('hourofday'))

def isMorning(x):
    if x is None:
        return 0
    if x >=6 and x <=9:
        return 1
    else:
        return 0
isMorningUdf =  udf (isMorning, IntegerType())
featureeddf = featureeddf.withColumn("Morning",isMorningUdf('hourofday'))

dfLen = featureeddf.count()
featureeddf.persist()
###############################
# lag features
previousWeek=int(24*7)
previousMonth=int(24*365.25/12)
lags=[48, 49, 50, 51, 52, 55, 60, 67, 72, 96]
lags.extend([previousWeek, previousMonth])
lagColumns = ['peakLoad']
for i in lags:
    wSpec = Window.partitionBy('ServerIP').orderBy('SessionStartHourTime')
    for j in lagColumns:
        featureeddf = featureeddf.withColumn(j+'Lag'+str(i),lag(featureeddf[j], i).over(wSpec) )

mlSourceDF = featureeddf
mlSourceDF.printSchema()
mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in mlSourceDF.columns if 'Lag' in x])
# after creating all lag features, we can drop NA columns on the key columns
# drop na to avoid error in StringIndex 
mlSourceDF = mlSourceDF.na.drop(subset=["ServerIP","SessionStartHourTime"])
# indexing
columnsForIndex = ['dayofweek', 'ServerIP', 'year', 'month', 'weekofyear', 'dayofmonth', 'hourofday', 
                     'Holiday', 'BusinessHour', 'Morning']

mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in columnsForIndex ])

sIndexers = [StringIndexer(inputCol=x, outputCol=x + '_indexed').setHandleInvalid("skip") for x in columnsForIndex]
indexModel = Pipeline(stages=sIndexers).fit(mlSourceDF)
mlSourceDF = indexModel.transform(mlSourceDF)
# save model for operationalization
indexModel.write().overwrite().save(stringIndexModelFile)

# encoding for categorical features
catVarNames=[x + '_indexed' for x in columnsForIndex ]

columnOnlyIndexed =   [ catVarNames[i] for i in range(0,len(catVarNames)) if len(indexModel.stages[i].labels)<2 ]
columnForEncode = [ catVarNames[i] for i in range(0,len(catVarNames)) if len(indexModel.stages[i].labels)>=2 ]

info['columnOnlyIndexed'] = columnOnlyIndexed
info['columnForEncode'] = columnForEncode

# save info to blob storage
write_blob(info, infoFile, storageContainer, storageAccount, storageKey)

ohEncoders = [OneHotEncoder(inputCol=x, outputCol=x + '_encoded')
              for x in columnForEncode ]
ohPipelineModel = Pipeline(stages=ohEncoders).fit(mlSourceDF)
mlSourceDFCat = ohPipelineModel.transform(mlSourceDF)

ohPipelineModel.write().overwrite().save(oneHotEncoderModelFile)


# feature scaling for numeric features
featuresForScale =  [x for x in mlSourceDFCat.columns if 'Lag' in x]
print(len(featuresForScale))
assembler = VectorAssembler(
  inputCols=featuresForScale, outputCol="features"
)

assembled = assembler.transform(mlSourceDFCat).select(col('key'), col('features'))

scaler = StandardScaler(
  inputCol="features", outputCol="scaledFeatures",
  withStd=True, withMean=False
).fit(assembled)

scaler.write().overwrite().save(featureScaleModelFile)

scaledData = scaler.transform(assembled).select('key','scaledFeatures')
def extract(row):
    return (row.key, ) + tuple(float(x) for x in row.scaledFeatures.values)

rdd = scaledData.rdd.map(lambda x: Row(key=x[0],scaledFeatures=DenseVector(x[1].toArray())))
scaledDf = rdd.map(extract).toDF(["key"])
# rename columns
oldColumns = scaledDf.columns
scaledColumns = ['scaledKey']
scaledColumns.extend(['scaled'+str(i) for i in featuresForScale])
scaledOutcome = scaledDf.select([col(oldColumns[index]).alias(scaledColumns[index]) for index in range(0,len(oldColumns))])
noScaledMLSourceDF = mlSourceDFCat.select([column for column in mlSourceDFCat.columns if column not in featuresForScale])
newDF = noScaledMLSourceDF.join(scaledOutcome, noScaledMLSourceDF.key==scaledOutcome.scaledKey, 'outer')
newDF.cache()
mlSourceDFCat = newDF
mlSourceDFCat=mlSourceDFCat.fillna(0, subset= [x for x in mlSourceDFCat.columns if 'Lag' in x])
mlSourceDFCat=mlSourceDFCat.fillna(0, subset= ['linearTrend'])
## save the intermediate result for downstream work
mlSourceDFCat.write.mode('overwrite').parquet(mlSourceDFFile)
#spark.stop()

