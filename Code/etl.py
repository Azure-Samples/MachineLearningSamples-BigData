#################################
# This script performs the data loading, data preparation and feature enginnering.
# It takes two arguments:
# 1. The configuration file which contains the Azure 
#    storage account name, key and data source location. 
#    By default, it is "./Config/storageconfig.json"
# 2. a DEBUG argument which is a string. 
#    If set to "FILTER_IP", the filtering down two IP addresses takes effect. 
#    By default, it is "FALSE". 
################################

import os
import sys
import time
import datetime
import json
from pandas.tseries.holiday import USFederalHolidayCalendar

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


from util import attach_storage_container, write_blob, read_blob
from data_prep import *
from feature_eng import *

from azureml.logging import get_azureml_logger


def encoding(mlSourceDF):       

    mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in mlSourceDF.columns if 'Lag' in x])
    # after creating all lag features, we can drop NA columns on the key columns
    # drop na to avoid error in StringIndex 
    mlSourceDF = mlSourceDF.na.drop(subset=["ServerIP","Time"])
    # indexing
    columnsForIndex = ['dayofweek', 'ServerIP', 'year', 'month', 'weekofyear', 'dayofmonth', 'hourofday', 
                     'Holiday', 'BusinessHour', 'Morning']

    mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in columnsForIndex ])

    sIndexers = [StringIndexer(inputCol=x, outputCol=x + '_indexed').setHandleInvalid("skip") for x in columnsForIndex]
    indexModel = Pipeline(stages=sIndexers).fit(mlSourceDF)
    mlSourceDF = indexModel.transform(mlSourceDF)
    # save model for operationalization

    # encoding for categorical features
    catVarNames=[x + '_indexed' for x in columnsForIndex ]

    columnOnlyIndexed =   [ catVarNames[i] for i in range(0,len(catVarNames)) if len(indexModel.stages[i].labels)<2 ]
    columnForEncode = [ catVarNames[i] for i in range(0,len(catVarNames)) if len(indexModel.stages[i].labels)>=2 ]

    info['columnOnlyIndexed'] = columnOnlyIndexed
    info['columnForEncode'] = columnForEncode

    ohEncoders = [OneHotEncoder(inputCol=x, outputCol=x + '_encoded')
              for x in columnForEncode ]
    ohPipelineModel = Pipeline(stages=ohEncoders).fit(mlSourceDF)
    mlSourceDFCat = ohPipelineModel.transform(mlSourceDF)

    return mlSourceDFCat, indexModel, ohPipelineModel



def scaling(mlSourceDFCat): 
    # feature scaling for numeric features
    featuresForScale =  [x for x in mlSourceDFCat.columns if 'Lag' in x]
    print(len(featuresForScale))
    assembler = VectorAssembler(
      inputCols=featuresForScale, outputCol="features"
    )

    assembled = assembler.transform(mlSourceDFCat).select(col('h_key2'), col('features'))

    scaler = StandardScaler(
      inputCol="features", outputCol="scaledFeatures",
      withStd=True, withMean=False
    ).fit(assembled)

    scaler.transform(assembled).printSchema()
    scaledData = scaler.transform(assembled).select('h_key2','scaledFeatures')
    scaledData.printSchema()

    def extract(row):
        return (row.rddKey, ) + tuple(float(x) for x in row.scaledFeatures.values)

    rdd = scaledData.rdd.map(lambda x: Row(rddKey=x[0],scaledFeatures=DenseVector(x[1].toArray())))
    
    scaledDf = rdd.map(extract).toDF(["rddkey"])    
    # rename columns
    oldColumns = scaledDf.columns
    scaledColumns = ['scaledKey']
    scaledColumns.extend(['scaled'+str(i) for i in featuresForScale])
    scaledOutcome = scaledDf.select([col(oldColumns[index]).alias(scaledColumns[index]) for index in range(0,len(oldColumns))])
    noScaledMLSourceDF = mlSourceDFCat.select([column for column in mlSourceDFCat.columns if column not in featuresForScale])
    result = noScaledMLSourceDF.join(scaledOutcome, noScaledMLSourceDF.h_key2==scaledOutcome.scaledKey, 'outer')
    
    result=result.fillna(0, subset= [x for x in result.columns if 'Lag' in x])
    result=result.fillna(0, subset= ['linearTrend'])
    return result, scaler



if __name__ == '__main__':
    global spark, info
    #mlSourceDFFile, stringIndexModelFile, oneHotEncoderModelFile, featureScaleModelFile, infoFile
    sys.path.append("./")
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

    # attach the blob storage to the spark cluster or VM so that the storage can be accessed by the cluste or VM        
    attach_storage_container(spark, storageAccount, storageKey)
    # print runtime versions
    print ('****************')
    print ('Python version: {}'.format(sys.version))
    print ('Spark version: {}'.format(spark.version))
    print(spark.sparkContext.getConf().getAll())
    print ('****************')




    # load csv files in blob storage into Spark dataframe
    print(time.time())
    run_logger.log("reading file from ", dataFile)
    df = loadData(spark,dataFile)
    print(time.time())


    # rename the columns
    newdf = renameColumns(df)
    if DEBUG == "FILTER_IP":
        newdf = filterDf(newdf)
    # get stats
    hourlyDf = findPeakInHour(spark, newdf)
    dailyDf = getHourlyMeanPerDay(spark, hourlyDf)

    timeHourlyDf = getAllHours(spark, info['trainBegin'], info['trainEnd'])
    timeDailyDf = getAllDays(spark, info['trainBegin'], info['trainEnd'])

    bucketDailyDf = createDailyBuckets(dailyDf, timeDailyDf) 
    featureDailyDf = bucketDailyDf.join(dailyDf, dailyDf.d_key==bucketDailyDf.d_key2, 'outer')
    featureDailyDf = addLagForDailyFeature(featureDailyDf)
    featureDailyDf = featureDailyDf.select([x for x in featureDailyDf.columns if x not in ['Time', 'ServerIP'] ])
    
    bucketHourlyDf = createHourlyBuckets(dailyDf, timeHourlyDf) 
    featureHourlyDf = bucketHourlyDf.join(hourlyDf, hourlyDf.h_key==bucketHourlyDf.h_key2, 'outer')

    # add day feature
    day = 3600*24
    day_window = F.from_unixtime(F.unix_timestamp('Time') - F.unix_timestamp('Time') %day)
    featureHourlyDf = featureHourlyDf.withColumn('SessionStartDay', day_window)

    featureHourlyDf = featureHourlyDf.withColumn("h_key_join", concat(featureHourlyDf.ServerIP,lit("_"),featureHourlyDf.SessionStartDay))

    featureDf = featureHourlyDf.join(featureDailyDf, featureHourlyDf.h_key_join==featureDailyDf.d_key2, "outer")
    featureDf = addLagForHourlyFeature(featureDf)

    mlSourceDF = addTimeFeature(featureDf,info['trainBegin'])
   
    encodedDf, indexModel, ohPipelineModel = encoding(mlSourceDF)
    # save info to blob storage
    write_blob(info, infoFile, storageContainer, storageAccount, storageKey)

    indexModel.write().overwrite().save(stringIndexModelFile)
    ohPipelineModel.write().overwrite().save(oneHotEncoderModelFile)
    result,scaler = scaling(encodedDf) 
    scaler.write().overwrite().save(featureScaleModelFile)
    result.write.mode('overwrite').parquet(mlSourceDFFile)
