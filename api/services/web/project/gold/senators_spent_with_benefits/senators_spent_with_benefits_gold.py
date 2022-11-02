from ...utils.reponse_generator import generateResponse
from pyspark.sql import functions as f

import logging

class SenatorsSpentWithBenefitsGold():
    def __init__(self, spark):
        self.spark = spark

    def execute(self):
        try:
            logging.info("START GOLD PROCESS IN SENATORS SPENT WITH BENEFITS")
            senators = self.spark.read.parquet("project/database/silver/senators")
            benefits = self.spark.read.parquet("project/database/silver/benefits")
            senators_benefits = senators.join(benefits, f.upper(f.col("NomeParlamentar")) == f.upper(f.col("SENADOR")), how="inner")

            senators_spent_benefits = senators_benefits.select("NomeCompletoParlamentar", "VALOR_REEMBOLSADO")\
                        .groupBy("NomeCompletoParlamentar")\
                        .agg(f.sum("VALOR_REEMBOLSADO").alias("VALOR_REEMBOLSADO"))\
                        .orderBy("VALOR_REEMBOLSADO", ascending=False)
            
            senators_spent_benefits.repartition(1).write.mode("overwrite")\
                .parquet("project/database/gold/senators_spent_with_benefits")

            return generateResponse(msg="Senators spent with benefits worte in file system")
        except Exception as exp:
            logging.error(f"ERROR: {exp}")

            return generateResponse("ERROR", str(exp))