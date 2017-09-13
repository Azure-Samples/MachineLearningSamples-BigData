import numpy as np
import pandas as pd
import os
import json
import urllib
import sys
import time

import subprocess
import re
import atexit
import imp


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



from azureml.logging import get_azureml_logger

# initialize logger
run_logger = get_azureml_logger()

    
configFilename = "./Config/storageconfig.json"

if len(sys.argv) > 1:
    configFilename = sys.argv[1]

with open(configFilename) as configFile:    
    config = json.load(configFile)
    global storageAccount, storageContainer, storageKey
    storageAccount = config['storageAccount']
    storageContainer = config['storageContainer']
    storageKey = config['storageKey']

# path to access the intermediate compute results and  the models
path = "wasb://{}@{}.blob.core.windows.net/".format(storageContainer, storageAccount)

# use the following two as the range to  calculate the holidays in the range of  [holidaBegin, holidayEnd ]
holidayBegin = '2009-01-01'
holidayEnd='2016-06-30'


    

mlSourceDFFile = path + 'vmlSource.parquet'
stringIndexModelFile = path + 'vstringIndexModel'
oneHotEncoderModelFile = path + 'voneHotEncoderModel'
featureScaleModelFile = path + 'vfeatureScaleModel'
mlModelFile = path + 'vmlModel'
infoFile = "info.pickle"


# start Spark session
spark = pyspark.sql.SparkSession.builder.appName('classification').getOrCreate()
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

#info = read_blob('local.info.pickle',infoFile,storageContainer, storageAccount, storageKey)
infoDf = spark.read.csv(path+infoFile, header=True, sep=',', inferSchema=True, nanValue="", mode='PERMISSIVE')
info = infoDf.rdd.map(lambda x: (x[0], x[1])).collectAsMap()
columnOnlyIndexed = [ x.strip() for x in info['columnOnlyIndexed'][1:-1].split(',') ]
columnForEncode = [ x.strip() for x in info['columnForEncode'][1:-1].split(',') ] 
trainBegin = info['trainBegin']
trainEnd   = info['trainEnd']
testSplitStart = info['testSplitStart']
duration = info['duration']
if duration == 'FULL':
    lowThreshhold = 100*7
    highThreshold = 500*7
else:
    lowThreshhold = 100
    highThreshold = 500



################################

# load result from 
mlSourceDFCat = spark.read.parquet(mlSourceDFFile)
#####################################
##Label data
def LoadtoLabel(num):
    if num is None: return 1
    if num <= lowThreshhold: return 1
    elif lowThreshhold < num and num <= highThreshold: return 2
    else: return 3
    
LoadtoLabelUdf = udf(LoadtoLabel, IntegerType())
mlSourceDFCat = mlSourceDFCat.withColumn("label", LoadtoLabelUdf(mlSourceDFCat['peakLoad']))

########################
training = mlSourceDFCat.filter(mlSourceDFCat.SessionStartHourTime < lit(testSplitStart).cast(TimestampType()) )
testing = mlSourceDFCat.filter(mlSourceDFCat.SessionStartHourTime >= lit(testSplitStart).cast(TimestampType()) )
print(training.count())
print(testing.count())
training.groupby('label').count().show()
testing.groupby('label').count().show()

##########################
# encode
input_features = ['linearTrend']
input_features.extend([x  for x in columnOnlyIndexed if len(x) > 0 ])
input_features.extend([x + '_encoded' for x in columnForEncode if len(x) > 0]) 
input_features.extend([x for x in mlSourceDFCat.columns if 'Lag' in x ])

print(input_features)

#######################
##machine learning
va = VectorAssembler(inputCols=input_features, outputCol='features')
training_assembled = va.transform(training).select('ServerIP', 'SessionStartHourTime','label','features')
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(numTrees=100, maxDepth=8, seed=42, labelCol="label", featuresCol="features" )
model = rf.fit(training_assembled)
numFeatures = model.numFeatures
print("num of features is %g" % numFeatures)

model.write().overwrite().save(mlModelFile)
## evaluate training result
pred_class_rf = model.transform(training_assembled.select(col('features'),col('Label')))
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator_train = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="Label")
accuracy = evaluator_train.evaluate(pred_class_rf, {evaluator_train.metricName: "accuracy"})
weightedPrecision = evaluator_train.evaluate(pred_class_rf, {evaluator_train.metricName: "weightedPrecision"})
weightedRecall = evaluator_train.evaluate(pred_class_rf, {evaluator_train.metricName: "weightedRecall"})
F1 = evaluator_train.evaluate(pred_class_rf, {evaluator_train.metricName: "f1"})
print("Train Accuracy = %g" % accuracy)
print("Train Weighted Precision = %g" % weightedPrecision)
print("Train Weighted Recall = %g" % weightedRecall)
print("Train F1 = %g" % F1)

run_logger.log("Train Accuracy", accuracy)
run_logger.log("Train Weighted Precision", weightedPrecision)
run_logger.log("Train Weighted Recall", weightedRecall)
run_logger.log("Train F1", F1)

## evaluate test result
test_assembled = va.transform(testing).select('ServerIP', 'SessionStartHourTime','label','features')
pred_test_class_rf = model.transform(test_assembled.select(col('features'),col('label')))
#predictions_rf.groupby('peakload', 'prediction').count().show()
predictionAndLabels = pred_test_class_rf.select("label", "prediction")
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="label")
testAccuracy = evaluator.evaluate(pred_test_class_rf, {evaluator.metricName: "accuracy"})
testWeightedPrecision = evaluator.evaluate(pred_test_class_rf, {evaluator.metricName: "weightedPrecision"})
testWeightedRecall = evaluator.evaluate(pred_test_class_rf, {evaluator.metricName: "weightedRecall"})
testF1 = evaluator.evaluate(pred_test_class_rf, {evaluator.metricName: "f1"})

print("Test Accuracy = %g" % testAccuracy)
print("Test Weighted Precision = %g" % testWeightedPrecision)
print("Test Weighted Recall = %g" % testWeightedRecall)
print("Test F1 = %g" % testF1)

run_logger.log("Test Accuracy", testAccuracy)
run_logger.log("Test Weighted Precision", testWeightedPrecision)
run_logger.log("Test Weighted Recall", testWeightedRecall)
run_logger.log("Test F1", testF1)

spark.stop()

