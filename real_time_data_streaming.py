import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

pathJarDir = os.path.join("/Users/swagatranjit/Documents", "jars")
listjar = [os.path.join(pathJarDir, x) for x in os.listdir(pathJarDir)]
print(listjar)

kafka_topic = "registered_user"
kafka_bootstrap_servers = 'localhost:9092'
postgresql_hostName = "localhost"
postgresql_portNo = "5432"
postgresql_username = "postgres"
postgresql_password = "  "
postgresql_database = "amazon"
postgresql_driver = "org.postgresql.Driver"

database_properties = {}
database_properties['user'] = postgresql_username
database_properties['password'] = "  "
database_properties['driver'] = postgresql_driver

def write_to_table(current_df, epoc_id, table):
    try:
        jdbc_url = "jdbc:postgresql://" + postgresql_hostName + ":" + str(postgresql_portNo) + "/" + postgresql_database
        #Save the dataframe to the table.
        current_df.write.jdbc(url = jdbc_url,
                              table = table,
                              mode = 'append',
                              properties = database_properties)
    except Exception as ex:
        print(ex)
    # print("Exit out of write_to_table function")

if __name__ == "__main__":
    print("Welcome to Log Analysis of ECommerce !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.jars", ",".join(listjar)) \
        .config("spark.jars", ",".join(listjar)) \
        .config("spark.executor.extraClassPath", "/Users/swagatranjit/Documents/jars/commons-pool2-2.8.1.jar:/Users/swagatranjit/Documents/postgresql-42.2.16.jar:/Users/swagatranjit/Documents/jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:/Users/swagatranjit/Documents/jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    items_df = spark \
        .readStream \
        .format("kafka") \
        .option("subscribe", kafka_topic) \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    items_df.printSchema()

    items_schema = StructType() \
        .add("IP", StringType()) \
        .add("category1", StringType()) \
        .add("category2", StringType()) \
        .add("page", StringType())

    items_df1 = items_df.selectExpr("CAST(value AS STRING)")
    items_df2 = items_df1.select(from_json(col("value"), items_schema).alias("items"))
    items_df3 = items_df2.select("items.*")

    print("Printing Schema of orders_df3: ")
    items_df3.printSchema()
    try:
        item_df4 = items_df3.groupBy("IP").count()
        orders_agg_write_stream = item_df4 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .option("truncate", "false")\
            .format("console") \
            .start()
        item_df4 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .foreachBatch(
            lambda current_df, epoc_id: write_to_table(current_df, epoc_id, "dashboard_ip")) \
            .start()

        item_df5 = items_df3.groupBy("category1").count()
        orders_agg_write_stream = item_df5 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .option("truncate", "false") \
            .format("console") \
            .start()

        item_df5 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .foreachBatch(
            lambda current_df, epoc_id: write_to_table(current_df, epoc_id, "dashboard_category1")) \
            .start()

        item_df6 = items_df3.groupBy("category2").count()
        orders_agg_write_stream = item_df6 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .option("truncate", "false") \
            .format("console") \
            .start()

        item_df6 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .foreachBatch(
            lambda current_df, epoc_id: write_to_table(current_df, epoc_id, "dashboard_category2")) \
            .start()

        item_df7 = items_df3.groupBy("page").count()
        orders_agg_write_stream = item_df7 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .option("truncate", "false") \
            .format("console") \
            .start()
        postgresql_table_name = "ecommerce"
        item_df7 \
            .writeStream \
            .trigger(processingTime='35 seconds') \
            .outputMode("update") \
            .foreachBatch(
            lambda current_df, epoc_id: write_to_table(current_df, epoc_id, "dashboard_page")) \
            .start()
        orders_agg_write_stream.awaitTermination()
    except Exception as ex:
        print(ex)

    print("Stream Data Processing Application Completed.")