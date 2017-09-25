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

from feature_eng import *
#from azureml.sdk import data_collector
trainBegin = '2009-01-01 00:00:00'

# use the following two as the range to  calculate the holidays in the range of  [holidaBegin, holidayEnd ]
holidayBegin = '2009-01-01'
holidayEnd='2016-06-30'

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
    IPList= {serverIP} 
    hourlyfeaturedf = hourlyfeaturedf.filter(hourlyfeaturedf["h_ServerIP"].isin(IPList) == True)
    dailyStatisticdf = dailyStatisticdf.filter(dailyStatisticdf["d_ServerIP"].isin(IPList) == True)
    
    
    hourlyfeaturedf.cache()    
    dailyStatisticdf.cache()
    return hourlyfeaturedf,dailyStatisticdf





    scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime = getScoreTime(scoreBegin)
    
   

#############################################
# mini batch scoring for serverIP with scoring start "scoreBegin"
#############################################
def miniBatchWebServiceScore(scoreBegin= '2016-07-01 00:00:00',serverIP='210.181.165.92'):    
    hourlyfeatureDf,dailyStatisticDf = readData(scoreBegin, serverIP, statsLocation)
    print("hourlyfeatureDf count: ",hourlyfeatureDf.count())
    print("dailyStatisticdf count: ",dailyStatisticDf.count())
scoreBegin, scoreEnd, featureBegin, scoreEndDateTime,featureBeginDateTime, featureEndDateTime = getScoreTime(scoreBe
gin)
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

    featureHourlyDf = featureHourlyDf.withColumn("h_key_join", concat(featureHourlyDf.ServerIP,lit("_"),feature
HourlyDf.SessionStartDay))

    featureDf = featureHourlyDf.join(featureDailyDf, featureHourlyDf.h_key_join==featureDailyDf.d_key2, "outer"
)
    featureDf = addLagForHourlyFeature(featureDf)
    featureDf= featureDf.filter(featureDf.Time >= lit(scoreBegin).cast(TimestampType()) ).filter(featureDf.Time <= lit(scoreEnd).cast(TimestampType()))
    
    mlSourceDF = addTimeFeature(featureDf,featureBegin)
 
    temp = mlSourceDF.toPandas()
    # make sure the key column exist in the json object
    temp = temp.set_index(['h_key']) 
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
