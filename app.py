
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
from flask_cors import CORS
from pymongo import MongoClient
import pprint
import requests
import json



def assign_tod(hr):
    #print(hr)
    times_of_day = {
    #'morning' : range(0,12),
    'morning' : range(0,12),
    #'lunch': range(12,14),
    'lunch': range(12,14),
    #'afternoon': range(14,18),
    'afternoon': range(14,18),
    #'evening': range (18,20),
    'evening': range (18,20),
    #'night': range(20,24)
    'night': range(20,24)
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
#spark.stop()


    
def getDayStats():
    print("Getting Day stats")
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    print("posting df")
    df.printSchema()
    print(df.show())
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

    #create a function to return the string value of the time of day
    func_udf = udf(assign_tod, StringType())
    dfbuckets = typedbikerentaldfhour.withColumn('daypart',func_udf(typedbikerentaldfhour['starttimehour']))
    print("Serving Station Stats data")
    test=dfbuckets.groupBy("daypart").count()
    print("Printing group by daypart")
    print(test.show())
    resultlist=test.toJSON().collect()
    return resultlist
    
def getMobileOsStats():
    print("Getting Day stats")
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    print("posting df")
    df.printSchema()
    print(df.show())
    ###################### Getting mobileos stats #######################
    mobiledata = df.select(col("mobileos").alias("mobileos"))
    mobiledata=mobiledata.groupBy("mobileos").agg(count("*"))
    mobiledata = mobiledata.select(col("mobileos").alias("mobileos"),col("count(1)").alias("mobileoscount"))
    print (mobiledata.show())
    resultlist=mobiledata.toJSON().collect()
    print(resultlist)
    return resultlist
    
################### app Web Server #####################
app = Flask(__name__)
#CORS(app)
#CORS(app, resources=r'/*', headers='Content-Type')

@app.route("/")
def mainRoute():
    print("Serving /")
    return render_template("index.html")

@app.route("/getstationstats")
def dataRoute():
    results=getDayStats()
    print(results)
    json_results = json.dumps(results)
    print(json_results)
    return json_results

@app.route("/getmobileosstats")
def mobiledataRoute():
    print("Serving Mobile OS")
    results=getMobileOsStats()
    print(results)
    json_results = json.dumps(results)
    print(json_results)
    return json_results
    

#.defer(d3.json, "inserturl&metric_name=hits&since=2017-07-01&period=year&granularity=month&skip_change=true")  
# had to do it this way because CORS is not enabled by s-scale by default
@app.route("/gethits")
def hitsRoute():
    print("Getting 3-scale hits")
    response=requests.get("inserturl&metric_name=hits&since=2017-07-01&period=year&granularity=month&skip_change=true")
    print(response.status_code)
    print(response.headers)
    print(response.content)
    return (response.content)  

@app.route("/getpoststartrental")
def startRentalRoute():
    print("Getting 3-scale startrental")
    response=requests.get("inserturl&metric_name=poststartrental&since=2017-07-01&period=year&granularity=month&skip_change=true")
    print(response.status_code)
    print(response.headers)
    print(response.content)
    return (response.content) 

@app.route("/getpoststoprental")
def stopRentalRoute():
    print("Getting 3-scale stoprental")
    response=requests.get("inserturl&metric_name=poststoprental&since=2017-07-01&period=year&granularity=month&skip_change=true")
    print(response.status_code)
    print(response.headers)
    print(response.content)
    return (response.content) 
    

@app.route("/traindata")
def trainRoute():
    print("Training data")
    # Read Data from database
    #mongoClient = MongoClient('mongodb://admin:admin@mongodb/sampledb')
    # run learning on apache spark
    
    return null

####### Allowing access-control
#@app.after_request
#def after_request(response):
#  response.headers.add('Access-Control-Allow-Origin', '*')
#  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
#  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
#  return response


    
    
print("HTTP Server started")
app.run(host='0.0.0.0', port=8080)


#spark.stop()
