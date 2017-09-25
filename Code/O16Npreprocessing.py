"""
The script is used to preprocessing the 2016 May and June raw data.
It generates the daily and hourly stats on the load features.
It's not required to run in this tutorial and can be used as reference for data pipelines.
"""
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
from pyspark.sql.types import DateType
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


from util import attach_storage_container
from data_prep import *


spark = pyspark.sql.SparkSession.builder.appName('O16NPreprosessing').getOrCreate()

month = 6
if len(sys.argv) > 1:
    month = sys.argv[1]

# load storage configuration
configFilename = "./Config/storageconfig.json"

if len(sys.argv) > 2:
    configFilename = sys.argv[2]


with open(configFilename) as configFile:
config = json.load(configFile)
    global storageAccount, storageContainer, storageKey, dataFile, duration
    storageAccount = config['storageAccount']
    storageContainer = config['storageContainer']
    storageKey = config['storageKey']
    dataFile = config['dataFile']
    duration = config['duration']
    print("storageContainer " + storageContainer)

# use the following two as the range to  calculate the holidays in the range of  [holidaBegin, holidayEnd ]
holidayBegin = '2009-01-01'
holidayEnd='2016-06-30'

import datetime

global featureBegin,featureEnd, DailyDFFile, HourlyDFFile
if month == 5:
    featureBegin = "2016-05-01 00:00:00"
    featureEnd = "2016-05-31 23:59:59"
    DailyDFFile = 'wasb://o16npublic@viennabigdalimitless.blob.core.windows.net/dailyfeature/2016/05/'
    HourlyDFFile = 'wasb://o16npublic@viennabigdalimitless.blob.core.windows.net/hourlyfeature/2016/05/'

if month == 6: 
    featuretureBegin = "2016-06-01 00:00:00"
    featureEnd = "2016-06-30 23:59:59"
    DailyDFFile = 'wasb://o16npublic@viennabigdalimitless.blob.core.windows.net/dailyfeature/2016/06/'
    HourlyDFFile = 'wasb://o16npublic@viennabigdalimitless.blob.core.windows.net/hourlyfeature/2016/06/'

BeginTimeStamp = int(datetime.datetime.strftime(datetime.datetime.strptime(featureBegin, "%Y-%m-%d %H:%M:%S") ,"%s"))
featureBeginDateTime = datetime.datetime.fromtimestamp(featureBeginTimeStamp)

featureStartFile = "wasb://publicdata@viennabigdalimitless.blob.core.windows.net/{0:04}/{1:02}/[a,b]/[0-6]/request.csv".format(featureBeginDateTime.year, featureBeginDateTime.month)

df = loadData(featureStartFile)

# rename the columns
newdf = renameColumns(df)
newdf = newdf.filter(newdf["ServerIP"]!='IPs')

# get stats
hourlyDf = findPeakInHour(spark, newdf)
dailyDf = getHourlyMeanPerDay(spark, hourlyDf)
# Extract some time features from "SessionStartHourTime" column
hourlyDf = hourlyDf.withColumn('dayofmonth', dayofmonth(houlyDf['SessionStartHourTime']))
hourlyDf.write.mode('overwrite').partitionBy("dayofmonth").parquet(HourlyDFFile)
dailyDf.write.mode('overwrite').parquet(DailyDFFile)
