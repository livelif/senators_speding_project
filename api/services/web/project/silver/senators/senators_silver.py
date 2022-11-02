import logging
from ...utils.reponse_generator import generateResponse
from pyspark.sql import functions as f

class SenatorsSilver():
    def __init__(self, spark):
        self.spark = spark

    def execute(self):
        try:
            df = self.spark.read.csv("project/database/bronze/senators", encoding="utf-8", header=True, sep=";", 
                    ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)
            senators = df.select(f.col("_c0").alias("row_number"), "codigoPalamentar", "NomeParlamentar", "NomeCompletoParlamentar")
            senators.repartition(1).write.mode("overwrite").parquet("project/database/silver/senators/")

            return generateResponse(msg="Senators wrote in silver layer")
        except Exception as exp:
            logging.error(f"ERROR: {exp}")
            return generateResponse("ERROR", str(exp))