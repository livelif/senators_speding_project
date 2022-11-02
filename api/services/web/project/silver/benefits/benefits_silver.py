from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from ...utils.reponse_generator import generateResponse
import logging

class Benefits():
    def __init__(self, spark):
        self.spark = spark

    def execute(self):
        try:
            schema = StructType([ 
                StructField("ANO", StringType(),True), \
                StructField("MES",StringType(),True), \
                StructField("SENADOR",StringType(),True), \
                StructField("TIPO_DESPESA",StringType(),True), \
                StructField("CNPJ_CPF", StringType(), True), \
                StructField("FORNECEDOR", StringType(), True), \
                StructField("DOCUMENTO", StringType(), True), \
                StructField("DATA", StringType(), True), \
                StructField("DETALHAMENTO", StringType(), True), \
                StructField("VALOR_REEMBOLSADO", StringType(), True), \
                StructField("COD_DOCUMENTO", StringType(), True)
            ])

            benefits_raw_df = self.spark.read.csv(f"project/database/bronze/benefits_pandas/", schema, sep = ";", header=True)

            benefits_silver_df = benefits_raw_df.withColumn("CNPJ_CPF", f.when(f.isnan(f.col("CNPJ_CPF")), "NONEXISTENT").otherwise(f.col("CNPJ_CPF")))
            benefits_silver_df = benefits_raw_df.withColumn("DOCUMENTO", f.when(f.isnan(f.col("DOCUMENTO")), "NONEXISTENT").otherwise(f.col("DOCUMENTO")))
            benefits_silver_df = benefits_raw_df.withColumn("DATA", f.when(f.isnan(f.col("DATA")), "NONEXISTENT").otherwise(f.col("DATA")))
            benefits_silver_df = benefits_raw_df.withColumn("VALOR_REEMBOLSADO", f.regexp_replace(f.col("VALOR_REEMBOLSADO"), ",", ".").cast("Double"))

            benefits_silver_df.repartition(1).write.mode('overwrite').parquet("project/database/silver/benefits/")
        
            return generateResponse(msg="Benefits wrote in silver layer")
        except Exception as exp:
            logging.info(f"ERROR: {exp}")

            return generateResponse("ERROR", str(exp))