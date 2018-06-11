from pyspark.sql import SparkSession

def getSession(local = True):
	if (local):
		spark =  SparkSession.builder.appName("DataMashup 2018 pySpark workshop").config("spark.master", "local[*]").getOrCreate()
		print ("Created local SparkSession")
		spark.read.option("header", "true").csv("../data/sales.csv").createOrReplaceTempView("sales")
		print ('Created "sales" view from CSV file')
		spark.read.option("header","true").csv("../data/item_prices.csv").createOrReplaceTempView("item_prices")
		print ('Created "item_prices" view from CSV file')
		return spark
	else:
		spark = SparkSession.builder.appName("workshop").config("spark.master", "yarn").enableHiveSupport().getOrCreate()
		print ("Created YARN SparkSession")
		return spark
