from ...utils.reponse_generator import generateResponse
from pyspark.sql import functions as f

import logging

class VotationBySenatorSilver():
    def __init__(self, spark):
        self.spark = spark
    
    def execute(self):
        try:
            df = self.spark.read.csv("project/database/bronze/votation_by_senator", encoding="ISO-8859-1", header=True, sep=",", 
                   ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)
            votation = df.select(f.col("_c0").alias("row_number"), "codigoPalamentar", "CodigoSessao", "CodigoSessaoLegislativa", "NumeroSessao", "DataSessao", "HoraInicioSessao", "CodigoMateria", "IdentificacaoProcesso")
            votation = votation.withColumn("DataSessao", f.to_date(f.col("DataSessao"), "yyyy-MM-dd"))
            votation = votation.withColumn("Year", f.year(f.col("DataSessao")))
            logging.info(votation.printSchema())
            votation.repartition(1).write.mode("overwrite").parquet("project/database/silver/votation_by_senator/")
            return generateResponse(msg="Votation By Senator wrote in silver layer")
        except Exception as exp:
            logging.error(f"ERROR: {exp}")
            return generateResponse("ERROR", str(exp))