from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("itemId", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("./customer-orders.csv")
df.printSchema()


amountByCustomerID = df.groupBy("customerID").agg(func.round(func.sum("amount"),2) \
    .alias("totalSpent"))

amountByCustomerID.show()

# Collect, format, and print the results
results = amountByCustomerID.sort("totalSpent")

results.show(results.count())
spark.stop()