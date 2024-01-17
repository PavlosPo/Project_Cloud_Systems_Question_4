import azure.functions as func
import logging
import tempfile
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType
from io import BytesIO

import time

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Q1 Vana") \
    .getOrCreate()

# Define your UDF
@udf(returnType=StringType())
def calculate_quadrant_udf(latitude, longitude):
    latitude = float(latitude)
    longitude = float(longitude)
    center_latitude = 37.7833
    center_longitude = -122.4167
    if latitude > center_latitude and longitude > center_longitude:
        return "Q1"
    elif latitude > center_latitude and longitude < center_longitude:
        return "Q2"
    elif latitude < center_latitude and longitude > center_longitude:
        return "Q3"
    elif latitude < center_latitude and longitude < center_longitude:
        return "Q4"
    else:
        return "Unknown Quadrant"

app = func.FunctionApp()



@app.blob_trigger(arg_name="myblob", path="mastercond/test_{number}.csv",
                               connection="AzureWebJobsStorage") 
def blob_trigger_demo_01_q4(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                 f"Name: {myblob.name}"
                 f"Blob Size: {myblob.length} bytes"
                 f"Blob Metadata: {myblob.metadata}")

     # Convert the InputStream to bytes and create a BytesIO object
    blob_content = BytesIO(myblob.read())

    # Convert BytesIO to string and split into lines
    blob_content_str = blob_content.getvalue().decode('utf-8')
    lines = blob_content_str.split('\n')

    # Convert lines to an RDD and then create a DataFrame
    rdd = spark.sparkContext.parallelize(lines)
    # df = spark.read.option("header", True).schema(schema).csv(rdd)
    df = spark.read.option("header", True).csv(rdd)

    # Use the UDF on the DataFrame
    df = df.withColumn("quadrant", calculate_quadrant_udf(df["pickup_latitude"], df["pickup_longitude"]))

    # Count the number of occurrences of each quadrant
    quadrant_counts = df.groupBy("quadrant").count()

    # Read the previous quadrant_counts from the CSV file (if it exists)
    try:
        previous_counts = spark.read.option("header", True).csv("quadrant_counts")
        # previous_counts.show()
        previous_counts = previous_counts.select("quadrant", previous_counts["count"].cast("double"))
        quadrant_counts = previous_counts.union(quadrant_counts).groupBy("quadrant").sum("count").withColumnRenamed("sum(count)", "count")
    except:
        pass

    quadrant_counts.show()

    # Save the DataFrame with specified headers to CSV
    quadrant_counts.write \
        .format("csv") \
        .option('header',"true") \
        .mode("overwrite") \
        .save("quadrant_counts")

