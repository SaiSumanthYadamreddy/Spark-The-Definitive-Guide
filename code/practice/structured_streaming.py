from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col

spark = SparkSession\
    .builder\
    .appName("StructStream")\
    .getOrCreate()

staticDataFrame = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load("/data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

staticDataFrame\
    .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")\
    .groupBy(
        col("CustomerId"), window(col("InvoiceData"), "1 day"))\
    .sum("total_cost")\
    .show(5)