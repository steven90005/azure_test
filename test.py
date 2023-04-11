import pyspark.sql.functions as fn
import json
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col , from_json , window ,max ,current_timestamp
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from ast import literal_eval
from pyspark.sql.functions import *
from kafka import KafkaProducer

driver = "org.postgresql.Driver"
database_host = "10.250.12.55"
database_port = "5432" 
database_name = "citus"
database_schema = "azpgpoc"
read_table = "steven_test"
write_teble = "pg_dwb_sink"
user = "pgdev"
password = "postgre@123"
url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

KafkaSchema = StructType([
    StructField("area", StringType()),
    StructField("array_fab_code", StringType()),
    StructField("array_fab_id", StringType()),
    StructField("array_lot_id", StringType()),
    StructField("auo_chip_id", StringType()),
    StructField("beol_fab_code", StringType()),
    StructField("beol_fab_id", StringType()),
    StructField("bl_id", StringType()),
    StructField("carton_no", StringType()),
    StructField("cell_chip_id", StringType()),
    StructField("cell_fab_code", StringType()),
    StructField("cell_fab_id", StringType()),
    StructField("cell_grade", StringType()),
    StructField("cell_part_no", StringType()),
    StructField("chip_qty", IntegerType()),
    StructField("complete_type", StringType()),
    StructField("create_time", TimestampType()),
    StructField("customer_barcode", StringType()),
    StructField("customer_barcode_ext", StringType()),
    StructField("customer_carton_no", StringType()),
    StructField("eqp_id", StringType()),
    StructField("event_id", StringType()),
    StructField("fab_code", StringType()),
    StructField("fab_id", StringType()),
    StructField("feol_fab_code", StringType()),
    StructField("feol_fab_id", StringType()),
    StructField("from_create_time", TimestampType()),
    StructField("grade", StringType()),
    StructField("identify", StringType()),
    StructField("input_grade", StringType()),
    StructField("input_part_no", StringType()),
    StructField("is_output_ng", StringType()),
    StructField("is_repair", StringType()),
    StructField("is_scrap", StringType()),
    StructField("line_id", StringType()),
    StructField("lm_time", TimestampType()),
    StructField("lm_user", StringType()),
    StructField("lot_comment", StringType()),
    StructField("lot_no", StringType()),
    StructField("lot_type", StringType()),
    StructField("mc_collect_create_time", TimestampType()),
    StructField("mc_dev_create_time", TimestampType()),
    StructField("mfg_date", TimestampType()),
    StructField("model_no", StringType()),
    StructField("next_op_cat", StringType()),
    StructField("next_op_id", StringType()),
    StructField("op_cat", StringType()),
    StructField("op_id", StringType()),
    StructField("op_rework_count", IntegerType()),
    StructField("pallet_no", StringType()),
    StructField("panel_repair_count", IntegerType()),
    StructField("parea", StringType()),
    StructField("part_no", StringType()),
    StructField("previous_op_id", StringType()),
    StructField("process_stage", StringType()),
    StructField("rma_no", StringType()),
    StructField("run_id", StringType()),
    StructField("scrap_yield_flag", StringType()),
    StructField("sheet_id", StringType()),
    StructField("shift_id", StringType()),
    StructField("shipping_no", StringType()),
    StructField("source_create_time", TimestampType()),
    StructField("src_schema_id", StringType()),
    StructField("start_dtm", StringType()),
    StructField("station_id", StringType()),
    StructField("track_in_time", TimestampType()),
    StructField("track_in_user", StringType()),
    StructField("track_out_time", TimestampType()),
    StructField("track_out_user", StringType()),
    StructField("wo_id", StringType()),
    StructField("wo_type", StringType())])

spark_for_test = SparkSession.builder.appName("SparkFinal_V2").getOrCreate()



def parsedRowDataFromJson(df):
    
    parsed_df_row = df \
        .selectExpr("CAST(value AS STRING)",\
                    "CAST(partition AS INTEGER)",\
                    "CAST(offset AS INTEGER)",\
                    "CAST(topic AS STRING)",\
                    "CAST(timestamp AS TIMESTAMP)")\
        .select(from_json("value",KafkaSchema).alias("data")\
                ,col("partition").alias("kafka_partition")\
                ,col("offset").alias("kafka_offset")\
                ,col("topic").alias("kafka_topic")\
                ,col("timestamp").alias("ts"))\
        .select("data.*","kafka_partition","kafka_offset","kafka_topic","ts")
    
    return parsed_df_row


def Get_Kafka_Consumer():
    
    df_stream_read_kafka = spark_for_test \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "") \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='' password='';") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("subscribe", "azure_topic") \
            .option("startingOffsets", "latest") \
            .option("kafka.group.id", "Azure-Consumer-Group-104") \
            .option("failOnDataLoss", "false") \
            .load() 
    
    df = parsedRowDataFromJson(df_stream_read_kafka)
    
    return df

def MappingJson(df,epoch_id):
    
    df_new = df.select(col("line_id"))
    
    return df_new
        

def SendToKafka(df):
    
    df_stream_read_kafka = df\
                .groupBy("op_id","line_id")\
                .agg(max("kafka_offset").alias("last_offset"),\
                     count("*").alias("count"),\
                     last("model_no").alias("current_model_no"),\
                     last("mfg_date").alias("current_mfg_day"),\
                     last("ts").alias("current_ts"),\
                     last("shift_id").alias("current_shift_id")) \
                .withColumn("db_insert_time",current_timestamp())\
                .withColumn("value",col("op_id"))
    
    df_stream_read_kafka\
        .writeStream\
        .foreachBatch(MappingJson)\
        .outputMode("update")\
        .format("kafka")\
        .option("topic", "azure_topic_get")\
        .option("kafka.bootstrap.servers", "") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='' password='';") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("checkpointLocation", "checkpoint_steven\checkpointToKafka_c9")\
        .start()

def DoLogicRun():
    
    df_row = Get_Kafka_Consumer()
    
    SendToKafka(df_row)

if __name__ == "__main__":
    DoLogicRun()  

