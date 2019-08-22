from pyspark.sql import SparkSession
import os, sys, pyspark

def getSession(local = True):
	os.environ['PYSPARK_PYTHON'] = sys.executable
	spark_home = os.path.dirname(pyspark.__file__)
	# Workaround for spark bug
	os.makedirs(spark_home + "\\RELEASE", exist_ok=True)

	if (local):
		spark =  SparkSession.builder.appName("pySpark for Data Science  workshop")\
			.config("spark.master", "local[*]")\
			.config("spark.home", spark_home)\
			.config("spark.pyspark.python", sys.executable).getOrCreate()
		print ("Created local SparkSession")
		spark.read.option("header", "true").csv("../data/sales.csv").createOrReplaceTempView("sales")
		print ('Created "sales" view from CSV file')
		spark.read.option("header","true").csv("../data/item_prices.csv").createOrReplaceTempView("item_prices")
		print ('Created "item_prices" view from CSV file')
		return spark
	else:
		spark.stop()
		spark = SparkSession.builder.appName("workshop").config("spark.master", "yarn").enableHiveSupport().getOrCreate()
		print ("Created YARN SparkSession")
		return spark
