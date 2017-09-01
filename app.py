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

