
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GeneralizedLinearRegression,LinearRegression
from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
import pprint



def assign_tod(hr):
    #print(hr)
    times_of_day = {
    #'morning' : range(0,12),
    0 : range(0,12),
    #'lunch': range(12,14),
    1: range(12,14),
    #'afternoon': range(14,18),
    2: range(14,18),
    #'evening': range (18,20),
    3: range (18,20),
    #'night': range(20,24)
    4: range(20,24)
    }
    for k, v in times_of_day.iteritems():
        if hr in v:
            #print k
            return k

 
print("Start")
#on openshift
spark = SparkSession.builder.appName("mobileanalytics").config("spark.mongodb.input.uri", "mongodb://admin:admin@mongodb/sampledb.bikerental").config("spark.mongodb.output.uri", "mongodb://admin:admin@mongodb/sampledb.bikerental").getOrCreate()
#local
#spark = SparkSession.builder.master("local").appName("mobileanalytics").config("spark.driver.bindAddress", "127.0.0.1").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sampledb.bikerental").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sampledb.bikerental").getOrCreate()


print("Started Spark")
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
print("posting df")
df.printSchema()
print(df.show())
# Create a model to predict timeofday bike rental count. 
# Morning, lunch , afternoon, evening
def assign_tod(hr):
    #print(hr)
    times_of_day = {
    #'morning' : range(0,12),
    0 : range(0,12),
    #'lunch': range(12,14),
    1: range(12,14),
    #'afternoon': range(14,18),
    2: range(14,18),
    #'evening': range (18,20),
    3: range (18,20),
    #'night': range(20,24)
    4: range(20,24)
    }
    for k, v in times_of_day.iteritems():
        if hr in v:
            #print k
            return k


#Convert starttime from string to timestamp
#'yyyy-MM-dd HH:mm:ss'
typedbikerentaldf= df.select(unix_timestamp(df.starttime).cast('timestamp').alias('starttimehour'),\
    'startstationid')\
    .cache()
# we only need the hour to put rentals in buckets of morning, lunch, afternoon, evening by station id
typedbikerentaldfhour= typedbikerentaldf.select(hour('starttimehour').alias('starttimehour'),\
    'startstationid')\
    .cache()
print(typedbikerentaldfhour.show())
typedbikerentaldfhour.printSchema()

#create a function to return the integer value of the time of day
func_udf = udf(assign_tod, IntegerType())
dfbuckets = typedbikerentaldfhour.withColumn('daypart',func_udf(typedbikerentaldfhour['starttimehour']))

df3=dfbuckets.groupBy("daypart", "startstationid").agg(count("*"))
df3=df3.withColumn("startstationid", dfbuckets["startstationid"].cast("integer"))
df3 = df3.select(col("daypart").alias("daypart"),col("startstationid").alias("startstationid"),col("count(1)").alias("rentalcount"))

print(df3.show())
df3.printSchema()
print("Number of rows")
#print(df3.count())

#group by daypart
#df3=df3.groupBy("daypart").agg(count("*"))
#print(df3.show())
#df3.printSchema()
#print("Number of rows")
df3.orderBy(df3.startstationid.desc())

print(df3.show())
df3.printSchema()
#spark.stop()

######################
#mongoClient = MongoClient('mongodb://admin:admin@mongodb')
#db= mongoClient.sampledb
#collection=db.bikerentalmembership
#for rental in collection.find():
 #   pprint.pprint(rental)

################### app Web Server #####################
app = Flask(__name__)

@app.route("/")
def mainRoute():
    print("Serving /")
    return render_template("index.html")

@app.route("/getstationstats")
def dataRoute():
    print("Serving Station Stats data")
    ########################### Testing toJOSON ###############
    #test=fullbikerentaldf.limit(10)
    #print("Printing TEST")
    #print(test.show())
    #resultlist=test.toJSON().collect()
    #print(resultlist)
    return null

@app.route("/traindata")
def trainRoute():
    print("Training data")
    # Read Data from database
    #mongoClient = MongoClient('mongodb://admin:admin@mongodb/sampledb')
    # run learning on apache spark
    
    return null
    
    
print("HTTP Server started")
app.run(host='0.0.0.0', port=8080)
