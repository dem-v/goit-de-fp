from pyspark.sql import SparkSession
from configs import config
from pyspark.sql.functions import col

def get_bio_df(existing_spark=None):
    # Use existing SparkSession if provided, otherwise create a new one
    if existing_spark:
        spark = existing_spark
    else:
        # Створення Spark сесії
        spark = (SparkSession.builder
                    .config("spark.jars", config["spark.jars"])
                    .master("local[*]")
                    .appName(config["spark.appName"])
                    .getOrCreate())

    # Читання даних з SQL бази даних
    df = (spark.read.format('jdbc').options(
            url=config["jdbc.url"],
            driver=config["jdbc.driver"],  # com.mysql.jdbc.Driver
            dbtable=config["jdbc.bio_table"],
            user=config["jdbc.user"],
            password=config["jdbc.password"])
        .load())

    bio_df = (df.select(col("athlete_id")
                        , col("name")
                        , col("sex")
                        , col("height").cast("float").alias("height")
                        , col("weight").cast("float").alias("weight")
                        , col("country_noc"))
              .where(col("height").isNotNull())
              .where(col("weight").isNotNull()))

    return bio_df


def get_events_df(existing_spark=None):
    # Use existing SparkSession if provided, otherwise create a new one
    if existing_spark:
        spark = existing_spark
    else:
        # Створення Spark сесії
        spark = (SparkSession.builder
                    .config("spark.jars", config["spark.jars"])
                    .master("local[*]")
                    .appName(config["spark.appName"])
                    .getOrCreate())

    # Set log level to view more detailed information
    #spark.sparkContext.setLogLevel("INFO")  # Options: "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"

    # Читання даних з SQL бази даних
    df = (spark.read.format('jdbc').options(
            url=config["jdbc.url"],
            driver=config["jdbc.driver"],  # com.mysql.jdbc.Driver
            dbtable=config["jdbc.events_table"],
            user=config["jdbc.user"],
            password=config["jdbc.password"])
          .option("numPartitions", 2)  # Adjust based on your MySQL server capacity
          .option("batchsize", 1000)  # Number of rows per batch
          .load())

    a_df = (df.select(col("country_noc")
                    , col("sport")
                    , col("athlete_id")
                    , col("medal")
                    , col("result_id")))

    return a_df

#df.show()
