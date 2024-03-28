import os
import re
import time
import env_config  # import env_config.py to set environment variables
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import SparkSession

# import directory paths from the env_config module
directory_path_camera = env_config.CAMERA_DATA_PATH
directory_path_temperature = env_config.TEMPERATURE_DATA_PATH


# process JSON files in the specified directory
def process_directory(directory_path):
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path) and file_path.endswith(".json"):
            with open(file_path, "r") as file:
                lines = file.readlines()
            modified_data = []
            for line in lines:
                modified_line = re.sub(r'"(\w+_id)": (\d+),', r'"\1": "\2",', line)
                modified_data.append(modified_line.rstrip('\n'))
            # write modified data back to the file
            with open(file_path, "w") as file:
                file.write('\n'.join(modified_data))
        else:
            print(f"Skipping file: {filename} (not a JSON file)")


# process camera and temperature data directory
process_directory(directory_path_camera)
process_directory(directory_path_temperature)

# initialize SparkSession
spark = SparkSession.builder \
    .appName("Data Safari") \
    .getOrCreate()

# define the schema for camera and temperature data
camera_schema = StructType([
    StructField("camera_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("animal_count", IntegerType(), True),
    StructField("species_count", IntegerType(), True),
    StructField("average_speed", IntegerType(), True)
])
temperature_schema = StructType([
    StructField("temperatureSensor_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", IntegerType(), True),
])

# define streaming DataFrame for camera data
streaming_camera_data = (
    spark
    .readStream
    .schema(camera_schema)
    .json(directory_path_camera)
)
# start streaming query to append mode and output to console
streaming_camera_query = streaming_camera_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

time.sleep(10)  # wait

# define streaming DataFrame for temperature data
streaming_temperature_data = (
    spark
    .readStream
    .schema(temperature_schema)
    .json(directory_path_temperature)
)
# start streaming query to append mode and output to console
streaming_temperature_query = streaming_temperature_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

time.sleep(10)  # wait

# calculate the average animal speed per camera in the last 10 minutes
camera_speed_avg = streaming_camera_data.groupBy(
    window("timestamp", "10 minutes"), "camera_id"
).agg(
    avg("average_speed").alias("average_speed"),  # calculate the average of the 'average_speed' column
    max("timestamp").alias("latest_timestamp")  # maximum timestamp in the window
).filter(
    col("latest_timestamp") > (current_timestamp() - expr("INTERVAL 10 MINUTES"))
)

# calculate the average temperature per sensor in the last 10 minutes
temperature_avg = streaming_temperature_data.groupBy(
    window("timestamp", "10 minutes"), "temperatureSensor_id"
).agg(
    avg("temperature").alias("average_temperature"),  # calculate the average of the 'temperature' column
    max("timestamp").alias("latest_timestamp")  # maximum timestamp in the window
).filter(
    col("latest_timestamp") > (current_timestamp() - expr("INTERVAL 10 MINUTES"))
)

# start streaming queries to console to see the results
streaming_query_camera_avg = camera_speed_avg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

time.sleep(10)  # wait

streaming_query_temperature_avg = temperature_avg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

time.sleep(10)  # wait

# read camera and temperature data as static DataFrames
static_camera_df = spark.read.schema(camera_schema).json(directory_path_camera)
static_temperature_df = spark.read.schema(temperature_schema).json(directory_path_temperature)

# join the camera data and temperature data based on timestamp and a common key
joined_df = static_camera_df.join(static_temperature_df, ["timestamp", "timestamp"], "inner")

# Calculate the correlation between temperature and number of species caught by the camera
correlation = joined_df.stat.corr("temperature", "species_count")

# Print the correlation coefficient
print("Correlation between temperature and number of species caught by the camera is:", correlation)

# Stop SparkSession
spark.stop()
