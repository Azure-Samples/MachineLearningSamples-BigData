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


spark = pyspark.sql.SparkSession.builder.appName('O16NPreprosessing').getOrCreate()

# use the following two as the range to  calculate the holidays in the range of  [holidaBegin, holidayEnd ]
trainBegin = '2016-06-01 00:00:00'
holidayBegin = '2009-01-01'
holidayEnd='2016-06-30'

import datetime
featureBegin = "2016-05-01 00:00:00"
featureEnd = "2016-05-31 23:59:59"
DailyDFFile = 'wasb://o16npublic1@viennabigdalimitless.blob.core.windows.net/dailyfeature/2016/05/'
HourlyDFFile = 'wasb://o16npublic1@viennabigdalimitless.blob.core.windows.net/hourlyfeature/2016/05/'

featureBeginTimeStamp = int(datetime.datetime.strftime(datetime.datetime.strptime(featureBegin, "%Y-%m-%d %H:%M:%S") ,"%s"))
featureBeginDateTime = datetime.datetime.fromtimestamp(featureBeginTimeStamp)

featureStartFile = "wasb://publicdata@viennabigdalimitless.blob.core.windows.net/{0:04}/{1:02}/[a,b]/[0-6]/request.csv".format(featureBeginDateTime.year, featureBeginDateTime.month)


dataFileSep = ','
df = spark.read.csv(featureStartFile, header=False, sep=dataFileSep, inferSchema=True, nanValue="", mode='PERMISSIVE')
df.cache()

# rename the columns
from functools import reduce
data = df.rdd
oldColumns = df.columns
newColumns=['TrafficType',"SessionStart","SessionEnd", "ConcurrentConnectionCounts", "MbytesTransferred",
            "ServiceGrade","HTTP1","ServerType",
            "SubService_1_Load","SubSerivce_2_Load", "SubSerivce_3_Load",
           "SubSerivce_4_Load", "SubSerivce_5_Load", "SecureBytes_Load", "TotalLoad", 'ServerIP', 'ClientIP']

newdf = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(0, len(oldColumns)), df)
newdf.printSchema()

newdf = newdf.filter(newdf["ServerIP"]!='IPs')

# add per five minutes feature
import pyspark.sql.functions as F
seconds = 300
seconds_window = F.from_unixtime(F.unix_timestamp('SessionStart') - F.unix_timestamp('SessionStart') % seconds)
newdf = newdf.withColumn('SessionStartFiveMin', seconds_window)

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
       """.format(featureBegin, featureEnd)
timeDf = spark.sql(sqlStatement)

timeDf=timeDf.withColumn("Time", col('col').cast(TimestampType()))

numRowsinTimeDF=timeDf.count()
timeDf.persist()

##########################
# join the timeDf to form dataframe with each per-five-minute bucket filled with proper data or null
joindf = timeDf.join(aggregatedf, aggregatedf.SessionStartFiveMin==timeDf.Time, "outer")


# add hour feature
secondsInHour = 3600
hour_window = F.from_unixtime(F.unix_timestamp('SessionStartFiveMin') - F.unix_timestamp('SessionStartFiveMin') %secondsInHour)
joindf = joindf.withColumn('SessionStartHourTime', hour_window.cast('timestamp'))

#aggregatedf = aggregatedf.withColumn('SessionStartHourTime', col('SessionStartHour').cast('timestamp'))

joindf = joindf.withColumn("key", concat(joindf.ServerIP,lit("_"),joindf.SessionStartHourTime.cast('string')))
joindf.cache()

joindf = joindf.fillna(0, subset=['SumTotalLoad'])
# add hour feature
secondsInHour = 3600
hour_window = F.from_unixtime(F.unix_timestamp('SessionStartFiveMin') - F.unix_timestamp('SessionStartFiveMin') %secondsInHour)
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

# Extract some time features from "SessionStartHourTime" column
from pyspark.sql.functions import year, month, dayofmonth,hour
featureeddf = featureeddf.withColumn('year', year(featureeddf['SessionStartHourTime']))
featureeddf = featureeddf.withColumn('month', month(featureeddf['SessionStartHourTime']))
featureeddf = featureeddf.withColumn('dayofmonth', dayofmonth(featureeddf['SessionStartHourTime']))
featureeddf = featureeddf.withColumn('hourofday', hour(featureeddf['SessionStartHourTime']))
featureeddf.write.mode('overwrite').partitionBy("dayofmonth").parquet(HourlyDFFile)


# add day feature
day = 3600*24
day_window = F.from_unixtime(F.unix_timestamp('SessionStartHourTime') - F.unix_timestamp('SessionStartHourTime') %
 day)
featureeddf = featureeddf.withColumn('SessionStartDay', day_window)



# aggregate daily
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
dailyStatisticdf = dailyStatisticdf.withColumn('year', year(dailyStatisticdf['d_SessionStartDay']))
dailyStatisticdf = dailyStatisticdf.withColumn('month', month(dailyStatisticdf['d_SessionStartDay']))
dailyStatisticdf.write.mode('overwrite').parquet(DailyDFFile)
