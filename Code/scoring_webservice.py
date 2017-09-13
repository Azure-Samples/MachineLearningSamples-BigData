import numpy as np
import pandas as pd
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


# Import Azure ML API SDK. The SDK is installed implicitly with the latest
# version of the CLI in your default python environment
#from azure.ml.api.schema.dataTypes import DataTypes
#from azure.ml.api.schema.sampleDefinition import SampleDefinition
#from azure.ml.api.realtime.services import generate_schema

import webservice

#os.environ["PYTHON_EGG_CACHE"] = "~/"

#from azureml.sdk import data_collector
trainBegin = '2009-01-01 00:00:00'
# initialize logger
#run_logger = data_collector.current_run() 
#def attach_storage_container(spark, account, key):
#    config = spark._sc._jsc.hadoopConfiguration()
#    setting = "fs.azure.account.key." + account + ".blob.core.windows.net"
#    if not config.get(setting):
#        config.set(setting, key)

FEATURE_COMPLEXITY = 2
# 0 is the simplest model
# 1 means with more statistic features
# 2 means with some rolling features and feature scaling 
# 3 means more rollong features with PCA

DEBUG = 'TRUE'
'FILTER_IP'
# 3 use a few IP 'FILTER_IP'

DURATION = 'ONE_MONTH'
# 'ONE_MONTH' use a month's data 
# 'ONE_YEAR' use a year's data  
# 'ALL_YEAR' use all year's data
# 'FULL' use full dataset with duplicated data copies

# use the following two as the range to  calculate the holidays in the range of  [holidaBegin, holidayEnd ]
holidayBegin = '2009-01-01'
holidayEnd='2016-06-30'



##################
##global variable
global  scoreBegin
global  featureBegin
global scoreEnd

global scaler
global ohPipelineModele
global mlModelFile
global mlModel

global df
global featureeddf
global newDf
global timeDf


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

def readData(scoreBegin,serverIP, path):

    scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime = getScoreTime(scoreBegin)
    featureStartFile = path + "hourlyfeature/{0:04}/{1:02}/*/".format(featureBeginDateTime.year, featureBeginDateTime.month)
    featureEndFile = path + "hourlyfeature/{0:04}/{1:02}/*/".format(featureEndDateTime.year, featureEndDateTime.month)
    print(featureStartFile)
    print(featureEndFile)
    dailyStartFile = path + "dailyfeature/{0:04}/{1:02}/".format(featureBeginDateTime.year, featureBeginDateTime.month)
    dailyEndFile = path + "dailyfeature/{0:04}/{1:02}/".format(featureEndDateTime.year, featureEndDateTime.month)
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
    
    IPList= {serverIP} #{'115.220.193.16','210.181.165.92'}
    hourlyfeaturedf = hourlyfeaturedf.filter(hourlyfeaturedf["ServerIP"].isin(IPList) == True)
    dailyStatisticdf = dailyStatisticdf.filter(dailyStatisticdf["d_ServerIP"].isin(IPList) == True)
    
    
    hourlyfeaturedf.cache()    
    dailyStatisticdf.cache()
    return hourlyfeaturedf,dailyStatisticdf



def getTimeDf(scoreBegin, serverIP):
    scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime = getScoreTime(scoreBegin) 
    # UDF
    def generate_date_series(start, stop, window=3600):
        begin = start - start%window
        end =   stop - stop%window + window
        return [begin + x for x in range(0, end-begin + 1, window)]  
    # Register UDF for later usage
    spark.udf.register("generate_date_series", generate_date_series, ArrayType(IntegerType()) )
    sqlStatement = """ SELECT explode(   generate_date_series( UNIX_TIMESTAMP('{0!s}', "yyyy-MM-dd HH:mm:ss"),
                 UNIX_TIMESTAMP('{1!s}', 'yyyy-MM-dd HH:mm:ss')) )
           """.format(featureBegin, scoreEnd)
    timeDf = spark.sql(sqlStatement)
    
    timeDf=  timeDf.withColumn("StartHour", col('col').cast('timestamp')) 
    timeDf = timeDf.withColumn("h_ServerIP", lit(serverIP))
    timeDf = timeDf.withColumn("h_key", concat(timeDf.h_ServerIP,lit("_"),timeDf.StartHour.cast('string')))
    
    def generate_day_series(start, stop, window=3600*24):
        begin = start - start%window
        end =   stop - stop%window + window
        return [begin + x for x in range(0, end-begin + 1, window)]  
    # Register UDF for later usage
    spark.udf.register("generate_date_series", generate_day_series, ArrayType(IntegerType()) )
    sqlStatement = """ SELECT explode(   generate_date_series( UNIX_TIMESTAMP('{0!s}', "yyyy-MM-dd HH:mm:ss"),
                 UNIX_TIMESTAMP('{1!s}', 'yyyy-MM-dd HH:mm:ss')) )
           """.format(featureBegin, scoreEnd)
    dailyTimeDf = spark.sql(sqlStatement)
    dailyTimeDf = dailyTimeDf.withColumn("daily_ServerIP", lit(serverIP))
    
    
    dailyTimeDf=dailyTimeDf.withColumn("d_StartDay", col('col').cast('timestamp'))
    dailyTimeDf = dailyTimeDf.withColumn("d_key", concat(dailyTimeDf.daily_ServerIP,lit("_"),dailyTimeDf.d_StartDay.cast('string')))
    
    print(timeDf.count())
    timeDf.persist()
    return dailyTimeDf,timeDf


    
def getLag(dailyTimeDf, timeDf, dailyStatisticdf, hourlyfeaturedf ):

    hourlyfeaturedf = timeDf.join(hourlyfeaturedf, hourlyfeaturedf.key == timeDf.h_key, "outer")
    hourlyfeaturedf.printSchema()
    dailyStatisticdf = dailyStatisticdf.withColumn("d_key1", concat(dailyStatisticdf.d_ServerIP,lit("_"),dailyStatisticdf.d_SessionStartDay.cast('string')))
    dailydf = dailyTimeDf.join(dailyStatisticdf, dailyTimeDf.d_key == dailyStatisticdf.d_key1, "outer" )
    
    #lag features
    #previous week average
    #rolling mean features
    rollingLags = [2] # use features that's 48 hours ahead
    lagColumns = [x for x in dailyStatisticdf.columns if 'Daily' in x]
    print(lagColumns)
    windowSize=[7]
    for w in windowSize:
        for i in rollingLags:
            wSpec = Window.partitionBy('daily_ServerIP').orderBy('d_StartDay').rowsBetween(-i-w, -i-1)
            for j in lagColumns:
                dailydf = dailydf.withColumn(j+'Lag'+str(i)+'Win'+str(w),F.avg(dailydf[j]).over(wSpec) )
    
    selectColumns = [x for x in dailydf.columns if 'year' not in x and 'month' not in x and x != 'col'  ]
    dailyDf = dailydf.select(selectColumns)
    
    
    
    #lag features    
    previousWeek=int(24*7)
    previousMonth=int(24*365.25/12)
    lags=[48, 49, 50, 51, 52, 55, 60, 67, 72, 96]
    lags.extend([previousWeek, previousMonth])
    lagColumns = ['peakLoad']
    for i in lags:
        wSpec = Window.partitionBy('h_ServerIP').orderBy('StartHour')
        for j in lagColumns:
            hourlyfeaturedf = hourlyfeaturedf.withColumn(j+'Lag'+str(i),lag(hourlyfeaturedf[j], i).over(wSpec) )  
    
    # add day feature

    day = 3600*24  
    day_window = F.from_unixtime(F.unix_timestamp('StartHour') - F.unix_timestamp('StartHour') % day)
    hourlyfeaturedf = hourlyfeaturedf.withColumn('StartDay', day_window)
    dailyDf = dailyDf.withColumn("d_key2", concat(dailyDf.d_ServerIP,lit("_"),dailyDf.d_StartDay.cast('string')))

    hourlyfeaturedf = hourlyfeaturedf.withColumn("d_key2", concat(hourlyfeaturedf.h_ServerIP,lit("_"),hourlyfeaturedf.StartDay.cast('string')))
    
    # Single column join is much faster than two columns join
    hourlyfeaturedf = hourlyfeaturedf.join(dailyDf,  (hourlyfeaturedf.d_key2 == dailyDf.d_key2),
                                   'outer' )

    hourlyfeaturedf.printSchema()
    hourlyfeaturedf = hourlyfeaturedf.select([x for x in hourlyfeaturedf.columns if 'd_' not in x and x != 'col'])

    
    return hourlyfeaturedf


def getFeature(hourlyfeaturedf,scoreBegin):
    featureeddf = hourlyfeaturedf 
    print(hourlyfeaturedf.columns)
    hourlyfeaturedf.show(5)
    scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime = getScoreTime(scoreBegin)
    
    featureeddf= featureeddf.filter(featureeddf.StartHour >= lit(scoreBegin).cast(TimestampType()) ).filter(featureeddf.StartHour < lit(scoreEnd).cast(TimestampType()))
    
    #featureeddf = featureeddf.withColumn("key", concat(featureeddf.h_ServerIP,lit("_"),featureeddf.StartHour.cast('string')))
    # Extract some time features from "SessionStartHourTime" column
    from pyspark.sql.functions import year, month, weekofyear, dayofmonth, date_format, hour
    featureeddf = featureeddf.withColumn('year', year(featureeddf['StartHour']))
    featureeddf = featureeddf.withColumn('month', month(featureeddf['StartHour']))
    featureeddf = featureeddf.withColumn('hourofday', hour(featureeddf['StartHour']))
 
    featureeddf = featureeddf.withColumn('weekofyear', weekofyear(featureeddf['StartHour']))
    dayofweek = F.date_format(featureeddf['StartHour'], 'EEEE')    
    featureeddf = featureeddf.withColumn('dayofweek', dayofweek )
    featureeddf = featureeddf.withColumn('dayofmonth', hour(featureeddf['StartHour']))
    
    import datetime
    trainBeginTimestamp = int(datetime.datetime.strftime(  datetime.datetime.strptime(trainBegin, "%Y-%m-%d %H:%M:%S") ,"%s"))
    def linearTrend(x):
        if x is None:
            return 0
        # return # of hour since the beginning
        return (x-trainBeginTimestamp)/3600/24/365.25
    # 
    linearTrendUdf =  udf(linearTrend,IntegerType())
    featureeddf = featureeddf.withColumn('linearTrend',linearTrendUdf(F.unix_timestamp('StartHour')))
    import pandas
    from pandas.tseries.holiday import USFederalHolidayCalendar
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
    featureeddf= featureeddf.withColumn('date', date_format(col('StartHour'), 'yyyy-MM-dd'))
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
    
    return featureeddf

def batchScore(scoreBegin= '2016-07-01 00:00:00',serverIP='210.181.165.92'):    
    webservice.init("./Model/")    
    dailyTimeDf,timeDf = getTimeDf(scoreBegin, serverIP)
    hourlyfeatureDf,dailyStatisticDf = readData(scoreBegin, serverIP, statsLocation)
    hourlyfeatureDf.show(5)
    print("hourlyfeatureDf count: ",hourlyfeatureDf.count())
    print("dailyStatisticdf count: ",dailyStatisticDf.count())
    lagDf = getLag(dailyTimeDf, timeDf, dailyStatisticDf, hourlyfeatureDf )
    lagDf.show()
    print("lagDf count", lagDf.count())
    featureDf = getFeature(lagDf,scoreBegin)
    print("featureDf count", featureDf.count())
    features =  [x for x in featureDf.columns if 'Lag' in x]
    features.extend(["h_key","StartHour","linearTrend"])
    columnsForIndex = ['dayofweek', 'h_ServerIP', 'year', 'month', 'weekofyear', 'dayofmonth', 'hourofday', 
                     'Holiday', 'BusinessHour', 'Morning']
    features.extend(columnsForIndex)
    featureDf = featureDf.select(features)
    featureDf = featureDf.withColumn("key", col("h_key"))
    featureDf = featureDf.withColumnRenamed("StartHour", "SessionStartHourTime")
    featureDf = featureDf.withColumnRenamed("h_ServerIP", "ServerIP")
    featureDf = featureDf.fillna(0, subset= [x for x in featureDf.columns if 'Lag' in x])
    #StartHour ->SessionStartHourTime
    #h_ServerIP -> ServerIP
    featureDf = featureDf.fillna(0, subset= ['linearTrend'])
    featureDf.printSchema()
    featureDf.show(5)
    temp = featureDf.toPandas()
    # make sure the key column exist in the json object
    temp = temp.set_index(['h_key']) 
    prediction=webservice.run(temp.to_json(orient='records'))
    print(prediction)

def miniBatchWebServiceScore(scoreBegin= '2016-07-01 00:00:00',serverIP='210.181.165.92'):    
    webservice.init("./Model/")    
    dailyTimeDf,timeDf = getTimeDf(scoreBegin, serverIP)
    hourlyfeatureDf,dailyStatisticDf = readData(scoreBegin, serverIP, statsLocation)
    hourlyfeatureDf.show(5)
    print("hourlyfeatureDf count: ",hourlyfeatureDf.count())
    print("dailyStatisticdf count: ",dailyStatisticDf.count())
    lagDf = getLag(dailyTimeDf, timeDf, dailyStatisticDf, hourlyfeatureDf )
    lagDf.show()
    print("lagDf count", lagDf.count())
    featureDf = getFeature(lagDf,scoreBegin)
    print("featureDf count", featureDf.count())
    features =  [x for x in featureDf.columns if 'Lag' in x]
    features.extend(["h_key","StartHour","linearTrend"])
    columnsForIndex = ['dayofweek', 'h_ServerIP', 'year', 'month', 'weekofyear', 'dayofmonth', 'hourofday', 
                     'Holiday', 'BusinessHour', 'Morning']
    features.extend(columnsForIndex)
    featureDf = featureDf.select(features)
    # make sure the schema is consistent
    featureDf = featureDf.withColumn("key", col("h_key"))
    #StartHour ->SessionStartHourTime
    #h_ServerIP -> ServerIP
    featureDf = featureDf.withColumnRenamed("StartHour", "SessionStartHourTime")
    featureDf = featureDf.withColumnRenamed("h_ServerIP", "ServerIP")
    featureDf = featureDf.fillna(0, subset= [x for x in featureDf.columns if 'Lag' in x])

    featureDf = featureDf.fillna(0, subset= ['linearTrend'])
    temp = featureDf.toPandas()
    # make sure the key column exist in the json object
    temp = temp.set_index(['h_key']) 
    #prediction=webservice.run(temp.to_json(orient='records'))
    prediction= consumePOSTRequestSync(url, temp.to_json(orient='records'), authorization)  #webservice.run(temp.to_json(orient='records'))
    return prediction
    
def consumePOSTRequestSync(url, data, authorization ):
    import requests
    #data = "[{\"peakLoadLag55\": 157.85000000000002, \"year\": 2016, \"peakLoadLag48\": 275.8000000000001, \"peakLoadLag50\": 382.2000000000001, \"peakLoadLag96\": 466.48000000000013, \"peakLoad5DailyLag2Win7\": 6.173369565217391, \"key\": \"210.181.165.92_2016-06-29 01:00:00\", \"peakLoadLag60\": 124.6, \"peakLoadDailyLag2Win7\": 317.9180615942029, \"dayofweek\": \"Wednesday\", \"peakBytesDailyLag2Win7\": 59.89416666666667, \"peakLoadLag730\": 429.1000000000001, \"Morning\": 0, \"peakLoad1DailyLag2Win7\": 277.55797101449275, \"weekofyear\": 26, \"month\": 6, \"linearTrend\": 0, \"peakLoadLag67\": 205.10000000000002, \"peakLoadLag49\": 466.9000000000002, \"peakLoadLag72\": 408.8000000000001, \"peakLoadLag168\": 219.80000000000007, \"peakLoad4DailyLag2Win7\": 16.995144927536234, \"peakLoadLag51\": 166.32000000000002, \"peakLoad3DailyLag2Win7\": 7.420289855072464, \"ServerIP\": \"210.181.165.92\", \"SessionStartHourTime\": \"2016-06-29 01:00:00\", \"peakLoadLag52\": 170.80000000000004, \"BusinessHour\": 0, \"peakLoad2DailyLag2Win7\": 5.212862318840579, \"peakLoadSecureDailyLag2Win7\": 4.477173913043477, \"hourofday\": 1, \"Holiday\": 0, \"dayofmonth\": 1}, {\"peakLoadLag55\": 117.6, \"year\": 2016, \"peakLoadLag48\": 243.88000000000002, \"peakLoadLag50\": 466.9000000000002, \"peakLoadLag96\": 2755.830000000001, \"peakLoad5DailyLag2Win7\": 6.173369565217391, \"key\": \"210.181.165.92_2016-06-29 02:00:00\", \"peakLoadLag60\": 468.30000000000007, \"peakLoadDailyLag2Win7\": 317.9180615942029, \"dayofweek\": \"Wednesday\", \"peakBytesDailyLag2Win7\": 59.89416666666667, \"peakLoadLag730\": 306.6, \"Morning\": 0, \"peakLoad1DailyLag2Win7\": 277.55797101449275, \"weekofyear\": 26, \"month\": 6, \"linearTrend\": 0, \"peakLoadLag67\": 219.10000000000002, \"peakLoadLag49\": 275.8000000000001, \"peakLoadLag72\": 357.4200000000001, \"peakLoadLag168\": 166.60000000000002, \"peakLoad4DailyLag2Win7\": 16.995144927536234, \"peakLoadLag51\": 382.2000000000001, \"peakLoad3DailyLag2Win7\": 7.420289855072464, \"ServerIP\": \"210.181.165.92\", \"SessionStartHourTime\": \"2016-06-29 02:00:00\", \"peakLoadLag52\": 166.32000000000002, \"BusinessHour\": 0, \"peakLoad2DailyLag2Win7\": 5.212862318840579, \"peakLoadSecureDailyLag2Win7\": 4.477173913043477, \"hourofday\": 2, \"Holiday\": 0, \"dayofmonth\": 2}]"
    
    #url = 'http://23.101.153.175:80/api/v1/service/load22/score'
    #-H "Content-Type:application/json" -H "Authorization:Bearer cb3f47eb7cde42968032b14b26c522be"
    #headers = {"Content-Type": "application/json", "Authorization": "Bearer cb3f47eb7cde42968032b14b26c522be"}
    headers = {"Content-Type": "application/json", "Authorization": authorization}
    # call get service with headers and params
    response = requests.post(url,data = data, headers=headers)
    if response.status_code == 200:
        return response.text
    else: 
        return None
    


    
if __name__ == '__main__':
    import json
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

    
    
    
    