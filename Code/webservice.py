#############################################
# This is the script for web service which uses the trained model for real-time scoring.
#############################################

import numpy as np
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
from pyspark.sql.dataframe import *
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.ml.classification import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer
from pyspark.ml.feature import StandardScaler, PCA, RFormula
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StandardScaler, StandardScalerModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.mllib.linalg import DenseVector
import datetime
import json
from pandas import DataFrame
import pickle


#############################################
# process the input data frame:
# (1) index the non-numeric  features, 
# (2) encode the indexed features and 
# (3) scale the numeric features
#############################################
def processDf(mlSourceDF):
    
    mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in mlSourceDF.columns if 'Lag' in x])
    mlSourceDF = mlSourceDF.na.drop(subset=["ServerIP","Time"])
    columnsForIndex = ['dayofweek', 'ServerIP', 'year', 'month', 'weekofyear', 'dayofmonth', 'hourofday', 
                     'Holiday', 'BusinessHour', 'Morning']
    mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in columnsForIndex ])
 
    # indexing    
    indexedDF = indexModel.transform(mlSourceDF)
    # encoding
    encodedDF = ohPipelineModel.transform(indexedDF)

    # feature scaling
    featuresForScale =  [x for x in encodedDF.columns if 'Lag' in x]
    assembler = VectorAssembler(
      inputCols=featuresForScale, outputCol="features"
    )
    assembled = assembler.transform(encodedDF).select('h_key2','features')
    scaledData = scaler.transform(assembled).select('h_key2','scaledFeatures')

    def extract(row):
        return (row.rddKey, ) + tuple(float(x) for x in row.scaledFeatures.values)
    rdd = scaledData.rdd.map(lambda x: Row(rddKey=x[0],scaledFeatures=DenseVector(x[1].toArray())))
    scaledDf = rdd.map(extract).toDF(["rddKey"])
    # rename columns
    oldColumns = scaledDf.columns
    scaledColumns = ['scaledKey']
    scaledColumns.extend(['scaled'+str(i) for i in featuresForScale])
    scaledOutcome = scaledDf.select([col(oldColumns[index]).alias(scaledColumns[index]) for index in range(0,len(oldColumns))])
    noScaledMLSourceDF = encodedDF.select([column for column in  encodedDF.columns if column not in featuresForScale])
    result = noScaledMLSourceDF.join(scaledOutcome, (noScaledMLSourceDF.h_key2==scaledOutcome.scaledKey), 'outer')
    return result
    
#############################################
# make prediciton based on the input data frame
# The 1st argument is the loaded machine learning model
# The 2nd argument is the dataframe with proper features 
#############################################
def score(mlModel, ScoreDFCat):
    
    ScoreDFCat=ScoreDFCat.fillna(0, subset= [x for x in ScoreDFCat.columns if 'Lag' in x])
    ScoreDFCat=ScoreDFCat.fillna(0, subset= ['linearTrend'])
    
    scoring= ScoreDFCat
    scoring = scoring.withColumn("label", lit(0))
    input_features = ['linearTrend']
    
    columnOnlyIndexed = [ x.strip() for x in info['columnOnlyIndexed'] ]
    columnForEncode = [ x.strip() for x in info['columnForEncode'] ]
    
    input_features.extend([x  for x in columnOnlyIndexed if len(x) > 0 ])
    input_features.extend([x + '_encoded' for x in columnForEncode if len(x) > 0]) 
    input_features.extend([x for x in ScoreDFCat.columns if 'Lag' in x ]) 
    input_features.extend([x for x in scoring.columns if 'Lag' in x ])


    # Assemble features  !!! Values to assemble cannot be null
    va = VectorAssembler(inputCols=input_features, outputCol='features')
    scoring_assembled = va.transform(scoring).select('ServerIP', 'Time', 'features','label')
    pred_class_rf = mlModel.transform(scoring_assembled.select('features','label', 'ServerIP', 'Time'))

    # predictions_rf.groupby('peakload', 'prediction').count().show()
    predictionAndLabels = pred_class_rf.select("ServerIP", "Time", "prediction")
    return predictionAndLabels

####################################################
# Initialization of the web service:
# start spark session
# load the models
####################################################
def init(path="./"):
    global indexModel, ohPipelineModel, scaler, mlModel, info, spark
    # start spark session
    spark = pyspark.sql.SparkSession.builder.appName('scoring').getOrCreate()
    # load the models
    stringIndexModelFile = path + 'stringIndexModel'
    oneHotEncoderModelFile = path + 'oneHotEncoderModel'
    featureScaleModelFile = path + 'featureScaleModel'
    scaler = StandardScalerModel.load(featureScaleModelFile)
    ohPipelineModel =  PipelineModel.load(oneHotEncoderModelFile)
    indexModel = PipelineModel.load(stringIndexModelFile)
    
    mlModelFile = path +'mlModel'
    mlModel =  RandomForestClassificationModel.load(mlModelFile)
    
    infoFile = path + 'info' 
    info = None
    # load info 
    with open(infoFile, 'rb') as handle:
        info = pickle.load(handle)


####################################################
# Web service function:
# make prediciton based on the input data frame
####################################################    
def run(inputDf):
    try:
        print("receive input")
        temp = json.loads(inputDf)
        df = DataFrame.from_dict(temp)
        print(df.head(5))

        featuredf = spark.createDataFrame(df)
        print("featuredf is created")
        scoringDf = processDf(featuredf)
        prediction = score(mlModel,scoringDf).toPandas()
    except Exception as e:
        return (str(e))
    return prediction.to_json(orient='index')

            
if __name__ == '__main__':
    init('./Models/')
    inputString = "[{\"Time\":1467500400000,\"ServerIP\":\"210.181.165.92\",\"h_key2\":\"210.181.165.92_2016-07-02 23:00:00\",\"SessionStartDay\":\"2016-07-02 00:00:00\",\"peakLoadDailyLag2Win7\":0.0,\"peakBytesDailyLag2Win7\":0.0,\"peakLoad1DailyLag2Win7\":0.0,\"peakLoad2DailyLag2Win7\":0.0,\"peakLoad3DailyLag2Win7\":0.0,\"peakLoad4DailyLag2Win7\":0.0,\"peakLoad5DailyLag2Win7\":0.0,\"peakLoadSecureDailyLag2Win7\":0.0,\"peakLoadLag48\":173.6,\"peakLoadLag49\":209.3,\"peakLoadLag50\":467.67,\"peakLoadLag51\":352.8,\"peakLoadLag52\":296.8,\"peakLoadLag55\":417.48,\"peakLoadLag60\":168.7,\"peakLoadLag67\":124.6,\"peakLoadLag72\":280.7,\"peakLoadLag96\":381.85,\"peakLoadLag168\":417.9,\"peakLoadLag730\":260.12,\"year\":2016,\"month\":7,\"weekofyear\":26,\"dayofmonth\":2,\"hourofday\":23,\"dayofweek\":\"Saturday\",\"linearTrend\":0,\"Holiday\":0,\"BusinessHour\":0,\"Morning\":0}]"
    #inputString = "[{\"peakLoadLag55\": 157.85000000000002, \"year\": 2016, \"peakLoadLag48\": 275.8000000000001, \"peakLoadLag50\": 382.2000000000001, \"peakLoadLag96\": 466.48000000000013, \"peakLoad5DailyLag2Win7\": 6.173369565217391, \"h_key2\": \"210.181.165.92_2016-06-29 01:00:00\", \"peakLoadLag60\": 124.6, \"peakLoadDailyLag2Win7\": 317.9180615942029, \"dayofweek\": \"Wednesday\", \"peakBytesDailyLag2Win7\": 59.89416666666667, \"peakLoadLag730\": 429.1000000000001, \"Morning\": 0, \"peakLoad1DailyLag2Win7\": 277.55797101449275, \"weekofyear\": 26, \"month\": 6, \"linearTrend\": 0, \"peakLoadLag67\": 205.10000000000002, \"peakLoadLag49\": 466.9000000000002, \"peakLoadLag72\": 408.8000000000001, \"peakLoadLag168\": 219.80000000000007, \"peakLoad4DailyLag2Win7\": 16.995144927536234, \"peakLoadLag51\": 166.32000000000002, \"peakLoad3DailyLag2Win7\": 7.420289855072464, \"ServerIP\": \"210.181.165.92\", \"Time\": \"2016-06-29 01:00:00\", \"peakLoadLag52\": 170.80000000000004, \"BusinessHour\": 0, \"peakLoad2DailyLag2Win7\": 5.212862318840579, \"peakLoadSecureDailyLag2Win7\": 4.477173913043477, \"hourofday\": 1, \"Holiday\": 0, \"dayofmonth\": 1}, {\"peakLoadLag55\": 117.6, \"year\": 2016, \"peakLoadLag48\": 243.88000000000002, \"peakLoadLag50\": 466.9000000000002, \"peakLoadLag96\": 2755.830000000001, \"peakLoad5DailyLag2Win7\": 6.173369565217391, \"h_key2\": \"210.181.165.92_2016-06-29 02:00:00\", \"peakLoadLag60\": 468.30000000000007, \"peakLoadDailyLag2Win7\": 317.9180615942029, \"dayofweek\": \"Wednesday\", \"peakBytesDailyLag2Win7\": 59.89416666666667, \"peakLoadLag730\": 306.6, \"Morning\": 0, \"peakLoad1DailyLag2Win7\": 277.55797101449275, \"weekofyear\": 26, \"month\": 6, \"linearTrend\": 0, \"peakLoadLag67\": 219.10000000000002, \"peakLoadLag49\": 275.8000000000001, \"peakLoadLag72\": 357.4200000000001, \"peakLoadLag168\": 166.60000000000002, \"peakLoad4DailyLag2Win7\": 16.995144927536234, \"peakLoadLag51\": 382.2000000000001, \"peakLoad3DailyLag2Win7\": 7.420289855072464, \"ServerIP\": \"210.181.165.92\", \"Time\": \"2016-06-29 02:00:00\", \"peakLoadLag52\": 166.32000000000002, \"BusinessHour\": 0, \"peakLoad2DailyLag2Win7\": 5.212862318840579, \"peakLoadSecureDailyLag2Win7\": 4.477173913043477, \"hourofday\": 2, \"Holiday\": 0, \"dayofmonth\": 2}]"
    prediction=run(inputString)
    print(prediction)
    
    
    
    
