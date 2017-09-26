##################################################
# This script is used to test the deployed service
# It takes one arguments:
# The configuration file which contains the Azure
#    storage account name, key and data source location.
#    By default, it is "./Config/webserivce.json"   
##################################################

import pyspark
import os
import urllib
import sys
import time

import subprocess
import re
import atexit
import imp

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, col, udf, lag, date_add, explode, lit, unix_timestamp
from pyspark.sql.functions import year, month, weekofyear, dayofmonth, hour, date_format 
from pyspark.sql.types import *
from pyspark.sql.types import DateType,TimestampType
from pyspark.sql.dataframe import *
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.ml.classification import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer
from pyspark.ml.feature import StandardScaler, PCA, RFormula
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler, StandardScalerModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
import datetime
import pandas
from pandas.tseries.holiday import USFederalHolidayCalendar
import requests
import json


#from azureml.sdk import data_collector
trainBegin = '2009-01-01 00:00:00'

# use the following two as the range to  calculate the holidays in the range of  [holidaBegin, holidayEnd ]
holidayBegin = '2009-01-01'
holidayEnd='2016-06-30'


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

###############################################
# generate the timestamps related to the scroing
##############################################
def getScoreTime(scoreBegin):
    scoreBeginTimeStamp = int(datetime.datetime.strftime(  datetime.datetime.strptime(scoreBegin, "%Y-%m-%d %H:%M:%S") ,"%s"))
    scoreEndTimeStamp =  scoreBeginTimeStamp + 2*24*3600 -1
    featureBeginTimeStamp = scoreBeginTimeStamp - (365.25/12)*24*3600 + 1
    featureEndTimeStamp = scoreBeginTimeStamp -1
    
    scoreEnd = datetime.datetime.fromtimestamp(
           scoreEndTimeStamp
        ).strftime('%Y-%m-%d %H:%M:%S')
    
    featureBegin = datetime.datetime.fromtimestamp(
           featureBeginTimeStamp
        ).strftime('%Y-%m-%d %H:%M:%S')
    
    scoreEndDateTime = datetime.datetime.fromtimestamp(scoreEndTimeStamp)
    featureBeginDateTime =   datetime.datetime.fromtimestamp(featureBeginTimeStamp)
    featureEndDateTime =   datetime.datetime.fromtimestamp(featureEndTimeStamp)
    return scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime

##################################################
# load the pre-computed hourly feature dataframe and
# daily feature dataframe
###################################################
def readData(scoreBegin,serverIP, path):
    #read daily statistics and hourly statistics from public container specified by path 
    scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime = getScoreTime(scoreBegin)
    featureStartFile = path + "hourlyfeature/{0:04}/{1:02}/*".format(featureBeginDateTime.year, featureBeginDateTime.month)
    featureEndFile = path + "hourlyfeature/{0:04}/{1:02}/*".format(featureEndDateTime.year, featureEndDateTime.month)
    print(featureStartFile)
    print(featureEndFile)
    dailyStartFile = path + "dailyfeature/{0:04}/{1:02}/*".format(featureBeginDateTime.year, featureBeginDateTime.month)
    dailyEndFile = path + "dailyfeature/{0:04}/{1:02}/*".format(featureEndDateTime.year, featureEndDateTime.month)
    print(dailyStartFile)
    print(dailyEndFile)
    
    hourlyfeaturedf = spark.read.parquet(featureStartFile)
    print(hourlyfeaturedf.columns)
    if featureStartFile!= featureEndFile:
        hourlyfeaturedf2 = spark.read.parquet(featureEndFile)
        print(hourlyfeaturedf2.columns)
        hourlyfeaturedf = hourlyfeaturedf.unionAll(hourlyfeaturedf2 )
    dailyStatisticdf = spark.read.parquet(dailyStartFile)
    print(dailyStatisticdf.columns)
    if dailyStartFile!= dailyEndFile:
        dailyStatisticdf2 = spark.read.parquet(dailyEndFile)
        print(dailyStatisticdf2.columns)
        dailyStatisticdf = dailyStatisticdf.unionAll(dailyStatisticdf2 )
   
    # filter down to the server IP of interest 
    IPList= set([serverIP])
    hourlyfeaturedf = hourlyfeaturedf.filter(hourlyfeaturedf["h_ServerIP"].isin(IPList) == True)
    dailyStatisticdf = dailyStatisticdf.filter(dailyStatisticdf["d_ServerIP"].isin(IPList) == True)
    
    
    hourlyfeaturedf.cache()    
    dailyStatisticdf.cache()
    return hourlyfeaturedf,dailyStatisticdf


   

#############################################
# mini batch scoring for serverIP with scoring start "scoreBegin"
#############################################
def miniBatchWebServiceScore(scoreBegin= '2016-07-01 00:00:00',serverIP='210.181.165.92'):    
    hourlyDf,dailyDf = readData(scoreBegin, serverIP, statsLocation)
    print("hourlyDf count: ",hourlyDf.count())
    print("dailydf count: ",dailyDf.count())
    scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime = getScoreTime(scoreBegin)
    timeHourlyDf = getAllHours(spark, featureBegin, scoreEnd)
    timeDailyDf = getAllDays(spark, featureBegin, scoreEnd)

    bucketDailyDf = createDailyBuckets(dailyDf, timeDailyDf)
    featureDailyDf = bucketDailyDf.join(dailyDf, dailyDf.d_key==bucketDailyDf.d_key2, 'outer')
    featureDailyDf = addLagForDailyFeature(featureDailyDf)
    featureDailyDf = featureDailyDf.select([x for x in featureDailyDf.columns if x not in ['Time', 'ServerIP']
])

    bucketHourlyDf = createHourlyBuckets(dailyDf, timeHourlyDf)
    featureHourlyDf = bucketHourlyDf.join(hourlyDf, hourlyDf.h_key==bucketHourlyDf.h_key2, 'outer')

    # add day feature
    day = 3600*24
    day_window = F.from_unixtime(F.unix_timestamp('Time') - F.unix_timestamp('Time') %day)
    featureHourlyDf = featureHourlyDf.withColumn('SessionStartDay', day_window)

    featureHourlyDf = featureHourlyDf.withColumn("h_key_join", concat(featureHourlyDf.ServerIP,lit("_"),featureHourlyDf.SessionStartDay))

    featureDf = featureHourlyDf.join(featureDailyDf, featureHourlyDf.h_key_join==featureDailyDf.d_key2, "outer"
)
    featureDf = addLagForHourlyFeature(featureDf)
    featureDf= featureDf.filter(featureDf.Time >= lit(scoreBegin).cast(TimestampType()) ).filter(featureDf.Time <= lit(scoreEnd).cast(TimestampType()))
    
    mlSourceDF = addTimeFeature(featureDf,featureBegin)
    mlSourceDF = mlSourceDF.select([x for x in mlSourceDF.columns if ('peak' in x and 'Lag' in x) or (x not in ['SessionStartHourTime', 'date', 'h_key', 'd_key', 'h_ServerIP','h_key_join', 'd_ServerIP'] and 'peak' not in x)])
    mlSourceDF.printSchema()
    mlSourceDF = mlSourceDF.fillna(0, subset= [x for x in mlSourceDF.columns if 'Lag' in x])
    mlSourceDF = mlSourceDF.fillna(0, subset= ['linearTrend'])
    columnsForIndex = ['dayofweek', 'ServerIP', 'year', 'month', 'weekofyear', 'dayofmonth', 'hourofday',
                     'Holiday', 'BusinessHour', 'Morning']

    mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in columnsForIndex ])
    mlSourceDF = mlSourceDF.withColumn('recordKey', col('h_key2'))
     
    temp = mlSourceDF.toPandas()
    # make sure the key column exist in the json object
    temp = temp.set_index(['recordKey']) 
    print(temp.to_json(orient='records'))
    prediction= consumePOSTRequestSync(url, temp.to_json(orient='records'), authorization)  #webservice.run(temp.to_json(orient='records'))
    return prediction

   
########################################
# call the web service to get prediction
######################################## 
def consumePOSTRequestSync(url, data, authorization ):
    headers = {"Content-Type": "application/json", "Authorization": authorization}
    # call get service with headers and params
    response = requests.post(url,data = data, headers=headers)
    if response.status_code == 200:
        return response.text
    else: 
        return None
    
    
if __name__ == '__main__':
    global spark, url, authorization, statsLocation
    configFilename = "./Config/webservice.json"

    if len(sys.argv) > 1:
        configFilename = sys.argv[1]
    
    with open(configFilename) as configFile:    
        config = json.load(configFile)
        url = config['url']
        authorization = config['authorization']
        statsLocation = config['statsLocation']

    spark = pyspark.sql.SparkSession.builder.appName('scoring').getOrCreate()
    print(miniBatchWebServiceScore('2016-07-01 00:00:00','210.181.165.92'))
