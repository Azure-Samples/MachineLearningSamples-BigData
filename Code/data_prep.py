import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, col, udf, lag, date_add, explode, lit, unix_timestamp


# load data
def loadData(spark,dataFile, dataFileSep=","):
    #run_logger.log("reading file from ", dataFile)
    df = spark.read.csv(dataFile, header=False, sep=dataFileSep, inferSchema=True, nanValue="", mode='PERMISSIVE')
    return df

# rename columns
def renameColumns(df, newColumns=None):
    
    # rename the columns
    oldnames = df.columns

    newColumns=['TrafficType',"SessionStart","SessionEnd", "ConcurrentConnectionCounts", "MbytesTransferred", 
            "ServiceGrade","HTTP1","ServerType", 
            "SubService_1_Load","SubSerivce_2_Load", "SubSerivce_3_Load", 
           "SubSerivce_4_Load", "SubSerivce_5_Load", "SecureBytes_Load", "TotalLoad", 'ServerIP', 'ClientIP']
    newdf = df.select([col(oldnames[index]).alias(newColumns[index]) for index in range(0,len(oldnames))])
    return newdf

# filter data
def filterDf(df, IPList=['115.220.193.16','210.181.165.92']):
    IPSet = set(IPList)
    newdf = df.filter(newdf["ServerIP"].isin(IPSet) == True)
    return newdf 

#####################################################
# find the peak per five minutes in a hour 
# for SumTotalLoad  feature 
#####################################################
def findPeakInHour(spark, df):
    
    # add per five minutes feature
    seconds = 300
    seconds_window = F.from_unixtime(F.unix_timestamp('SessionStart') - F.unix_timestamp('SessionStart') % seconds)    
    newdf = df.withColumn('SessionStartFiveMin', seconds_window.cast('timestamp'))

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
    
    # add hour feature
    secondsInHour = 3600  
    hour_window = F.from_unixtime(F.unix_timestamp('SessionStartFiveMin') - F.unix_timestamp('SessionStartFiveMin') % secondsInHour)
    aggregatedf = aggregatedf.withColumn('SessionStartHourTime', hour_window.cast('timestamp'))
    aggregatedf = aggregatedf.withColumn("h_key", concat(aggregatedf.ServerIP,lit("_"),aggregatedf.SessionStartHourTime.cast('string')))        
    
    # get the peakload every five minutes (non-overlapping) per hour
    maxByGroup = (aggregatedf.rdd
            .map(lambda x: (x[-1], x))  # Convert to PairwiseRD
            # Take maximum of the passed arguments by the last element (key)
            # equivalent to:
            # lambda x, y: x if x[-1] > y[-1] else y
            # 2 is the SumTotalLoad
            .reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[2])) 
           .values()) # Drop keys
    aggregatemaxdf = maxByGroup.toDF()
    aggregatemaxdf.createOrReplaceTempView("aggregatemaxdf")
    sqlStatement = """
        SELECT h_key, ServerIP h_ServerIP, SessionStartHourTime,
        SumTotalLoad peakLoad,
        SumMBytes peakBytes, 
        SumLoad1 peakLoad1, SumLoad2 peakLoad2, SumLoad3 peakLoad3,  
        SumLoad4 peakLoad4, SumLoad5 peakLoad5, SumLoadSecure peakLoadSecure
    FROM aggregatemaxdf 
    """
    hourlyDf = spark.sql(sqlStatement);
    return hourlyDf

#####################################################
# Create daily mean for hourly load features 
#####################################################
def getHourlyMeanPerDay(spark, houlyDf):
    # add day feature
    day = 3600*24
    day_window = F.from_unixtime(F.unix_timestamp('SessionStartHourTime') - F.unix_timestamp('SessionStartHourTime') %day)
    houlyDf = houlyDf.withColumn('SessionStartDay', day_window)

    # aggregate daily
    houlyDf.createOrReplaceTempView("houlyDf")
    sqlStatement = """
        SELECT h_ServerIP d_ServerIP, SessionStartDay d_SessionStartDay,
        AVG(peakLoad) peakLoadDaily,
        AVG(peakBytes) peakBytesDaily,
        AVG(peakLoad1) peakLoad1Daily, AVG(peakLoad2) peakLoad2Daily, AVG(peakLoad3) peakLoad3Daily,
        AVG(peakLoad4) peakLoad4Daily, AVG(peakLoad5) peakLoad5Daily, AVG(peakLoadSecure) peakLoadSecureDaily
        FROM houlyDf group by h_ServerIP, SessionStartDay
    """
    dailyDf = spark.sql(sqlStatement)
    dailyDf = dailyDf.withColumn("d_key", concat(dailyDf.d_ServerIP,lit("_"),dailyDf.d_SessionStartDay.cast('string')))        
    return dailyDf

