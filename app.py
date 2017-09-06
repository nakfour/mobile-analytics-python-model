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

#master="spark://172.17.0.10:7077"
#master="spark://test-ui-route-myproject.173.230.141.17.xip.io:7077"
#master="local[*]"
#sconf = SparkConf().setAppName("ophicleide-worker").setMaster(master)
#to get rid of the warning
# multiple sparkContext
#sconf.set("spark.driver.allowMultipleContexts","true")
#spark = SparkContext(conf=sconf)

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
spark = SparkSession.builder.appName("mobileanalytics").getOrCreate()
print("Started Spark")
spark.stop()

######################
mongoClient = MongoClient('mongodb://admin:admin@mongodb')
db= mongoClient.sampledb
collection=db.bikerentalmembership
for rental in collection.find():
    pprint.pprint(rental)

################### app Web Server #####################
app = Flask(__name__)

@app.route("/")
def mainRoute():
    print("Serving /")
    return null

@app.route("/getdata")
def dataRoute():
    print("Serving data")
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
