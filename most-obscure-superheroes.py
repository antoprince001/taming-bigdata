from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/antonyprincej/projects/SparkCourse/Marvel+names.txt")

lines = spark.read.text("file:///Users/antonyprincej/projects/SparkCourse/Marvel+graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))


least_connection_count = connections.agg(func.min("connections")).first()[0]

least_connections = connections.filter(func.col("connections") == least_connection_count)

least_connections_with_names = least_connections.join(names, "id")

least_connections_with_names.select("name").show()
