"""
Kafka Streaming Consumer для обробки спортивних результатів
Архітектура: Kafka -> Spark Streaming -> [Kafka + MySQL]
Покращена версія з централізованим керуванням даними
"""

import logging
import signal
import sys
import time
from typing import Optional, Dict, Any
from contextlib import contextmanager
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.streaming import StreamingQuery

from configs import config

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ProcessingMetrics:
    """Метрики обробки для моніторингу"""
    batches_processed: int = 0
    total_records_processed: int = 0
    total_records_written_kafka: int = 0
    total_records_written_mysql: int = 0
    failed_batches: int = 0

    def log_summary(self):
        logger.info(f"Processing Summary - Batches: {self.batches_processed}, "
                   f"Records processed: {self.total_records_processed}, "
                   f"Kafka writes: {self.total_records_written_kafka}, "
                   f"MySQL writes: {self.total_records_written_mysql}, "
                   f"Failed batches: {self.failed_batches}")


class DataManager:
    """Централізований менеджер для роботи з даними"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._bio_df: Optional[DataFrame] = None
        self._events_df: Optional[DataFrame] = None

    def get_bio_df(self, force_reload: bool = False) -> DataFrame:
        """Отримання bio DataFrame з кешуванням"""
        if self._bio_df is None or force_reload:
            logger.info("Loading bio data from database...")
            self._bio_df = self._load_bio_data()
            self._bio_df.cache()

            record_count = self._bio_df.count()
            logger.info(f"Bio data loaded and cached: {record_count} records")

        return self._bio_df

    def get_events_df(self, force_reload: bool = False) -> DataFrame:
        """Отримання events DataFrame з кешуванням"""
        if self._events_df is None or force_reload:
            logger.info("Loading events data from database...")
            self._events_df = self._load_events_data()
            self._events_df.cache()

            record_count = self._events_df.count()
            logger.info(f"Events data loaded and cached: {record_count} records")

        return self._events_df

    def _load_bio_data(self) -> DataFrame:
        """Завантаження bio даних з бази"""
        df = (self.spark.read
              .format('jdbc')
              .options(
                  url=config["jdbc.url"],
                  driver=config["jdbc.driver"],
                  dbtable=config["jdbc.bio_table"],
                  user=config["jdbc.user"],
                  password=config["jdbc.password"],
                  numPartitions="2",  # Зменшено з 4 до 2
                  fetchsize="500"    # Зменшено з 1000 до 500
              )
              .load())

        return (df.select(
                    col("athlete_id"),
                    col("name"),
                    col("sex"),
                    col("height").cast("float").alias("height"),
                    col("weight").cast("float").alias("weight"),
                    col("country_noc")
                )
                .filter(col("height").isNotNull() & col("weight").isNotNull())
                .filter(col("athlete_id").isNotNull()))  # Додаткова валідація

    def _load_events_data(self) -> DataFrame:
        """Завантаження events даних з бази"""
        df = (self.spark.read
              .format('jdbc')
              .options(
                  url=config["jdbc.url"],
                  driver=config["jdbc.driver"],
                  dbtable=config["jdbc.events_table"],
                  user=config["jdbc.user"],
                  password=config["jdbc.password"],
                  numPartitions="2",  # Зменшено з 4 до 2
                  fetchsize="500"    # Зменшено з 1000 до 500
              )
              .load())

        return df.select(
            col("country_noc"),
            col("sport"),
            col("athlete_id"),
            col("medal"),
            col("result_id")
        ).filter(col("athlete_id").isNotNull())

    def cleanup(self):
        """Очищення кешованих даних"""
        if self._bio_df:
            self._bio_df.unpersist()
        if self._events_df:
            self._events_df.unpersist()
        logger.info("Data cache cleared")


class SparkSessionManager:
    """Менеджер для керування Spark Session з оптимальними налаштуваннями"""

    @staticmethod
    def create_session(app_name: str = "AthleteResultsProcessor") -> SparkSession:
        """Створення оптимізованої Spark Session"""
        logger.info("Creating optimized Spark Session...")

        spark = (SparkSession.builder
                .appName(app_name)
                .master("local[2]")  # Обмежуємо до 2 ядер замість всіх
                .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
                .config("spark.driver.memory", "2g")  # Зменшено з 4g до 1g
                .config("spark.executor.memory", "2g")  # Зменшено з 4g до 1g
                .config("spark.driver.maxResultSize", "512m")  # Зменшено з 2g до 512m
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")  # Зменшено з 128MB
                .config("spark.sql.streaming.checkpointLocation.deletedFileRetention", "50")  # Зменшено з 100
                .config("spark.sql.streaming.minBatchesToRetain", "5")  # Зменшено з 10
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Вимкнено для економії пам'яті
                # Додаткові налаштування для економії пам'яті
                .config("spark.sql.streaming.metricsEnabled", "false")  # Вимкнути метрики
                .config("spark.sql.ui.retainedExecutions", "5")  # Зменшити retained executions
                .config("spark.ui.retainedJobs", "10")  # Зменшити retained jobs
                .config("spark.ui.retainedStages", "10")  # Зменшити retained stages
                .config("spark.worker.cleanup.enabled", "true")  # Увімкнути cleanup
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")  # Оптимізація shuffle
                .getOrCreate())

        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark Session created successfully - Version: {spark.version}")

        return spark


class KafkaConnector:
    """Клас для роботи з Kafka"""

    def __init__(self):
        self.kafka_options = self._build_kafka_options()

    def _build_kafka_options(self) -> Dict[str, str]:
        """Побудова опцій Kafka підключення"""
        return {
            "kafka.bootstrap.servers": config['kafka.bootstrap_servers'][0],
            "kafka.security.protocol": config['kafka.security_protocol'],
            "kafka.sasl.mechanism": config['kafka.sasl_mechanism'],
            "kafka.sasl.jaas.config": (
                f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                f"username='{config['kafka.username']}' "
                f"password='{config['kafka.password']}';"
            ),
            # Додаткові налаштування для стабільності
            "kafka.session.timeout.ms": "30000",
            "kafka.request.timeout.ms": "40000",
            "kafka.retry.backoff.ms": "1000"
        }

    def create_input_stream(self, spark: SparkSession) -> DataFrame:
        """Створення вхідного Kafka стріму"""
        logger.info(f"Creating Kafka input stream for topic: {config['kafka.in_topic']}")

        return (spark.readStream
                .format("kafka")
                .options(**self.kafka_options)
                .option("subscribe", config['kafka.in_topic'])
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", "20")  # Зменшено з 100 до 20
                .option("failOnDataLoss", "false")
                .option("includeHeaders", "false")  # Не включати заголовки якщо не потрібні
                .load())


def retry_on_failure(max_attempts: int = 3, delay: float = 1):
    """Декоратор для retry логіки"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"🔄 Retry {attempt + 1}/{max_attempts} for {func.__name__}: {str(e)}")
                        time.sleep(delay * (attempt + 1))  # Exponential backoff
                    else:
                        logger.error(f"💥 All {max_attempts} attempts failed for {func.__name__}")
            raise last_exception
        return wrapper
    return decorator


class DataTransformer:
    """Клас для трансформації даних"""

    @staticmethod
    def get_json_schema() -> StructType:
        """JSON схема для парсингу повідомлень"""
        return StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("country_noc", StringType(), True),
            StructField("result_id", IntegerType(), True)
        ])

    @classmethod
    def parse_kafka_messages(cls, df: DataFrame) -> DataFrame:
        """Парсинг Kafka повідомлень"""
        json_schema = cls.get_json_schema()

        return (df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .withColumn("value_json", from_json(col("value"), json_schema))
                .select(
                    coalesce(
                        col("value_json.result_id").cast("bigint"),
                        col("key").cast("bigint")
                    ).alias("result_id"),
                    col("value_json.athlete_id").alias("athlete_id"),
                    col("value_json.sport").alias("sport"),
                    col("value_json.medal").alias("medal"),
                    col("value_json.country_noc").alias("country_noc")
                )
                .filter(col("athlete_id").isNotNull() & col("sport").isNotNull()))

    @staticmethod
    def aggregate_athlete_stats(df: DataFrame, bio_df: DataFrame) -> DataFrame:
        """Агрегація статистики спортсменів"""
        return (df.join(bio_df, "athlete_id", "inner")
                .drop(bio_df.country_noc)  # Видаляємо country_noc з bio_df, залишаємо з основного df
                .select('sport', 'medal', 'sex', 'country_noc', 'height', 'weight')
                .withColumn("processing_timestamp", current_timestamp())
                .groupBy(['sport', 'medal', 'sex', 'country_noc'])
                .agg(
                    count("*").alias("total_athletes"),
                    avg("height").alias("avg_height"),
                    avg("weight").alias("avg_weight"),
                    min("height").alias("min_height"),
                    max("height").alias("max_height"),
                    min("weight").alias("min_weight"),
                    max("weight").alias("max_weight"),
                    first("processing_timestamp").alias("timestamp")
                ))

    @staticmethod
    def prepare_kafka_output(df: DataFrame) -> DataFrame:
        """Підготовка даних для виводу в Kafka"""
        return (df.withColumn(
                    "key",
                    concat(
                        col("sport"), lit("|"),
                        col("medal"), lit("|"),
                        col("sex"), lit("|"),
                        col("country_noc")
                    )
                )
                .select(
                    col("key").cast("string"),
                    to_json(struct(
                        "sport", "medal", "sex", "country_noc",
                        "total_athletes", "avg_height", "avg_weight",
                        "min_height", "max_height", "min_weight", "max_weight",
                        "timestamp"
                    )).alias("value")
                ))


class BatchProcessor:
    """Покращений процесор батчів"""

    def __init__(self, data_manager: DataManager, kafka_connector: KafkaConnector, metrics: ProcessingMetrics):
        self.data_manager = data_manager
        self.kafka_connector = kafka_connector
        self.metrics = metrics

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Основна логіка обробки батчу"""
        batch_start_time = time.time()

        try:
            logger.info(f"🔄 Processing batch {batch_id}")

            if batch_df.rdd.isEmpty():
                logger.info(f"📭 Batch {batch_id} is empty, skipping")
                return

            input_count = batch_df.count()
            logger.info(f"📥 Batch {batch_id}: {input_count} input records")

            # Отримання довідкових даних
            bio_df = self.data_manager.get_bio_df()

            # Агрегація даних
            aggregated_df = DataTransformer.aggregate_athlete_stats(batch_df, bio_df)

            if aggregated_df.rdd.isEmpty():
                logger.warning(f"⚠️ Batch {batch_id}: No records after aggregation")
                return

            output_count = aggregated_df.count()
            logger.info(f"📊 Batch {batch_id}: {output_count} aggregated records")

            # Показати результати для дебагу
            if logger.isEnabledFor(logging.DEBUG):
                aggregated_df.show(10, truncate=False)

            # Запис результатів
            kafka_success, mysql_success = self._write_results(aggregated_df, batch_id)

            # Оновлення метрик
            self.metrics.batches_processed += 1
            self.metrics.total_records_processed += input_count

            if kafka_success:
                self.metrics.total_records_written_kafka += output_count
            if mysql_success:
                self.metrics.total_records_written_mysql += output_count

            batch_time = time.time() - batch_start_time
            logger.info(f"✅ Batch {batch_id} completed in {batch_time:.2f}s")

        except Exception as e:
            self.metrics.failed_batches += 1
            logger.error(f"❌ Error processing batch {batch_id}: {str(e)}", exc_info=True)

    def _write_results(self, df: DataFrame, batch_id: int) -> tuple[bool, bool]:
        """Запис результатів у Kafka та MySQL"""
        kafka_success = self._write_to_kafka(df, batch_id)
        mysql_success = self._write_to_mysql(df, batch_id)
        return kafka_success, mysql_success

    @retry_on_failure(max_attempts=3, delay=2)
    def _write_to_kafka(self, df: DataFrame, batch_id: int) -> bool:
        """Запис у Kafka"""
        try:
            kafka_df = DataTransformer.prepare_kafka_output(df)

            (kafka_df.write
             .format("kafka")
             .options(**self.kafka_connector.kafka_options)
             .option("topic", config['kafka.out_topic'])
             .save())

            logger.info(f"📤 Kafka write successful for batch {batch_id}")
            return True

        except Exception as e:
            logger.error(f"📤❌ Kafka write failed for batch {batch_id}: {str(e)}")
            raise

    @retry_on_failure(max_attempts=3, delay=2)
    def _write_to_mysql(self, df: DataFrame, batch_id: int) -> bool:
        """Запис у MySQL"""
        try:
            (df.write
             .format("jdbc")
             .options(
                url=config["jdbc.url"],
                driver=config["jdbc.driver"],
                dbtable=config["jdbc.output_table"],
                user=config["jdbc.user"],
                password=config["jdbc.password"],
                batchsize="500",  # Зменшено з 1000 до 500
                isolationLevel="READ_UNCOMMITTED"  # Для кращої продуктивності
             )
             .mode("append")
             .save())

            logger.info(f"🗄️ MySQL write successful for batch {batch_id}")
            return True

        except Exception as e:
            logger.error(f"🗄️❌ MySQL write failed for batch {batch_id}: {str(e)}")
            raise


class StreamingApplication:
    """Головний клас додатку"""

    def __init__(self):
        self.spark: Optional[SparkSession] = None
        self.data_manager: Optional[DataManager] = None
        self.kafka_connector: Optional[KafkaConnector] = None
        self.streaming_query: Optional[StreamingQuery] = None
        self.metrics = ProcessingMetrics()
        self.shutdown_requested = False

        # Налаштування обробки сигналів
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Graceful shutdown handler"""
        logger.info(f"🛑 Received signal {signum}, initiating shutdown...")
        self.shutdown_requested = True

    def initialize(self):
        """Ініціалізація всіх компонентів"""
        logger.info("🚀 Initializing Streaming Application...")

        # Створення Spark Session
        self.spark = SparkSessionManager.create_session()

        # Ініціалізація менеджерів
        self.data_manager = DataManager(self.spark)
        self.kafka_connector = KafkaConnector()

        # Прогрівання кешу даних
        self.data_manager.get_bio_df()

        logger.info("✅ Application initialized successfully")

    def start_streaming(self):
        """Запуск streaming обробки"""
        logger.info(f"▶️ Starting streaming for topic: {config['kafka.in_topic']}")

        # Створення стріму
        input_stream = self.kafka_connector.create_input_stream(self.spark)
        transformed_stream = DataTransformer.parse_kafka_messages(input_stream)

        # Створення batch processor
        batch_processor = BatchProcessor(self.data_manager, self.kafka_connector, self.metrics)

        # Запуск streaming query
        self.streaming_query = (
            transformed_stream.writeStream
            .foreachBatch(batch_processor.process_batch)
            .outputMode("update")
            .option("checkpointLocation", "/tmp/athlete-streaming-checkpoints")
            .trigger(processingTime="30 seconds")  # Збільшено з 20 до 30 секунд
            .start()
        )

        logger.info(f"🎯 Streaming query started - ID: {self.streaming_query.id}")

        # Моніторинг
        self._monitor_streaming()

    def _monitor_streaming(self):
        """Моніторинг streaming job"""
        last_metrics_log = time.time()

        try:
            while not self.shutdown_requested and self.streaming_query.isActive:
                time.sleep(5)

                # Логуємо метрики кожні 60 секунд
                if time.time() - last_metrics_log > 60:
                    self.metrics.log_summary()
                    last_metrics_log = time.time()

        except KeyboardInterrupt:
            logger.info("⌨️ Keyboard interrupt received")
        finally:
            self._shutdown()

    def _shutdown(self):
        """Graceful shutdown"""
        logger.info("🛑 Shutting down application...")

        if self.streaming_query and self.streaming_query.isActive:
            logger.info("⏹️ Stopping streaming query...")
            self.streaming_query.stop()

        if self.data_manager:
            self.data_manager.cleanup()

        if self.spark:
            logger.info("🔌 Stopping Spark session...")
            self.spark.stop()

        # Фінальні метрики
        self.metrics.log_summary()
        logger.info("✅ Application shutdown completed")

    def run(self):
        """Основний метод запуску"""
        try:
            self.initialize()
            self.start_streaming()
        except Exception as e:
            logger.error(f"💥 Application error: {str(e)}", exc_info=True)
            raise
        finally:
            self._shutdown()


def main():
    """Точка входу"""
    app = StreamingApplication()

    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💀 Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()