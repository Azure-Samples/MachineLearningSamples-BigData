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
import json
from pandas import DataFrame


def processDf(df):
    mlSourceDF = df
    mlSourceDF.printSchema()
    mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in mlSourceDF.columns if 'Lag' in x])
    mlSourceDF = mlSourceDF.na.drop(subset=["ServerIP","SessionStartHourTime"])
    columnsForIndex = ['dayofweek', 'ServerIP', 'year', 'month', 'weekofyear', 'dayofmonth', 'hourofday', 
                     'Holiday', 'BusinessHour', 'Morning']
    mlSourceDF=mlSourceDF.fillna(0, subset= [x for x in columnsForIndex ])
    scoreDF = mlSourceDF 


    from pyspark.ml.feature import StringIndexer, OneHotEncoder
    # indexing    
    scoreDF = indexModel.transform(scoreDF)
    scoreDFCat = ohPipelineModel.transform(scoreDF)
    featuresForScale =  [x for x in scoreDFCat.columns if 'Lag' in x]
    assembler = VectorAssembler(
      inputCols=featuresForScale, outputCol="features"
    )
    assembled = assembler.transform(scoreDFCat).select('key','features')
    scaledData = scaler.transform(assembled).select('key','scaledFeatures')


    def extract(row):
        return (row.key, ) + tuple(float(x) for x in row.scaledFeatures.values)
    from pyspark.sql.types import Row
    from pyspark.mllib.linalg import DenseVector
    rdd = scaledData.rdd.map(lambda x: Row(key=x[0],scaledFeatures=DenseVector(x[1].toArray())))
    scaledDf = rdd.map(extract).toDF(["key"])
    # rename columns
    oldColumns = scaledDf.columns
    scaledColumns = ['scaledKey']
    scaledColumns.extend(['scaled'+str(i) for i in featuresForScale])
    scaledOutcome = scaledDf.select([col(oldColumns[index]).alias(scaledColumns[index]) for index in range(0,len(oldColumns))])
    scaledOutcome.show(1)
    scaledOutcome.cache()
    noScaledMLSourceDF = scoreDFCat.select([column for column in scoreDFCat.columns if column not in featuresForScale])
    noScaledMLSourceDF.cache()
    noScaledMLSourceDF.printSchema()
    scaledOutcome.printSchema()
    newDF = noScaledMLSourceDF.join(scaledOutcome, (noScaledMLSourceDF.key==scaledOutcome.scaledKey), 'outer')
    return newDF
    
def score(mlModel, newDF):
    ScoreDFCat = newDF
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
    #train_downsampled_assembled = va.transform(train_downsampled).select('ServerIP', 'SessionStartHourTime','Label','features')
    scoring_assembled = va.transform(scoring).select('ServerIP', 'SessionStartHourTime', 'features','label')
    
    pred_class_rf = mlModel.transform(scoring_assembled.select('features','label', 'ServerIP', 'SessionStartHourTime'))

    #predictions_rf.groupby('peakload', 'prediction').count().show()
    predictionAndLabels = pred_class_rf.select("ServerIP", "SessionStartHourTime", "prediction")
    return predictionAndLabels



def init(path="./"):
    global indexModel, ohPipelineModel, scaler, mlModel, info, spark

    spark = pyspark.sql.SparkSession.builder.appName('dd').getOrCreate()

    stringIndexModelFile = path + 'stringIndexModel'
    oneHotEncoderModelFile = path + 'oneHotEncoderModel'
    featureScaleModelFile = path + 'featureScaleModel'
    print(featureScaleModelFile) 
    scaler = StandardScalerModel.load(featureScaleModelFile)
    ohPipelineModel =  PipelineModel.load(oneHotEncoderModelFile)
    indexModel = PipelineModel.load(stringIndexModelFile)
    
    mlModelFile = path +'mlModel'
    mlModel =  RandomForestClassificationModel.load(mlModelFile)
    
    infoFile = path + 'info' 
    info = None
    #infoDf = spark.read.csv(path + 'info', header=True, sep=',', inferSchema=True, nanValue="", mode='PERMISSIVE')
    #info = infoDf.rdd.map(lambda x: (x[0], x[1])).collectAsMap()
    import pickle
    
    with open(infoFile, 'rb') as handle:
        info = pickle.load(handle)
    
def run(input_df):
    import json
    try:
        print("receive input")
        import json
        temp = json.loads(input_df)
        
        print(temp)

        from pandas import DataFrame
        df = DataFrame.from_dict(temp)
        print("df is loaded")
        print(df.head(5))

        featuredf = spark.createDataFrame(df)
        print("featuredf is created")
        scoringDf = processDf(featuredf)
        prediction = score(mlModel,scoringDf).toPandas()
    except Exception as e:
        return (str(e))
    return prediction.to_json(orient='index')

            
if __name__ == '__main__':
    init('./Model/')
    inputString = "[{\"peakLoadLag55\": 157.85000000000002, \"year\": 2016, \"peakLoadLag48\": 275.8000000000001, \"peakLoadLag50\": 382.2000000000001, \"peakLoadLag96\": 466.48000000000013, \"peakLoad5DailyLag2Win7\": 6.173369565217391, \"key\": \"210.181.165.92_2016-06-29 01:00:00\", \"peakLoadLag60\": 124.6, \"peakLoadDailyLag2Win7\": 317.9180615942029, \"dayofweek\": \"Wednesday\", \"peakBytesDailyLag2Win7\": 59.89416666666667, \"peakLoadLag730\": 429.1000000000001, \"Morning\": 0, \"peakLoad1DailyLag2Win7\": 277.55797101449275, \"weekofyear\": 26, \"month\": 6, \"linearTrend\": 0, \"peakLoadLag67\": 205.10000000000002, \"peakLoadLag49\": 466.9000000000002, \"peakLoadLag72\": 408.8000000000001, \"peakLoadLag168\": 219.80000000000007, \"peakLoad4DailyLag2Win7\": 16.995144927536234, \"peakLoadLag51\": 166.32000000000002, \"peakLoad3DailyLag2Win7\": 7.420289855072464, \"ServerIP\": \"210.181.165.92\", \"SessionStartHourTime\": \"2016-06-29 01:00:00\", \"peakLoadLag52\": 170.80000000000004, \"BusinessHour\": 0, \"peakLoad2DailyLag2Win7\": 5.212862318840579, \"peakLoadSecureDailyLag2Win7\": 4.477173913043477, \"hourofday\": 1, \"Holiday\": 0, \"dayofmonth\": 1}, {\"peakLoadLag55\": 117.6, \"year\": 2016, \"peakLoadLag48\": 243.88000000000002, \"peakLoadLag50\": 466.9000000000002, \"peakLoadLag96\": 2755.830000000001, \"peakLoad5DailyLag2Win7\": 6.173369565217391, \"key\": \"210.181.165.92_2016-06-29 02:00:00\", \"peakLoadLag60\": 468.30000000000007, \"peakLoadDailyLag2Win7\": 317.9180615942029, \"dayofweek\": \"Wednesday\", \"peakBytesDailyLag2Win7\": 59.89416666666667, \"peakLoadLag730\": 306.6, \"Morning\": 0, \"peakLoad1DailyLag2Win7\": 277.55797101449275, \"weekofyear\": 26, \"month\": 6, \"linearTrend\": 0, \"peakLoadLag67\": 219.10000000000002, \"peakLoadLag49\": 275.8000000000001, \"peakLoadLag72\": 357.4200000000001, \"peakLoadLag168\": 166.60000000000002, \"peakLoad4DailyLag2Win7\": 16.995144927536234, \"peakLoadLag51\": 382.2000000000001, \"peakLoad3DailyLag2Win7\": 7.420289855072464, \"ServerIP\": \"210.181.165.92\", \"SessionStartHourTime\": \"2016-06-29 02:00:00\", \"peakLoadLag52\": 166.32000000000002, \"BusinessHour\": 0, \"peakLoad2DailyLag2Win7\": 5.212862318840579, \"peakLoadSecureDailyLag2Win7\": 4.477173913043477, \"hourofday\": 2, \"Holiday\": 0, \"dayofmonth\": 2}]"
    prediction=run(inputString)
    print(prediction)
    
    
    
    
