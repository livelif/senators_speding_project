from ...utils.reponse_generator import generateResponse
from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType

import logging

class CompanyHaveMoreRevenueGold():
    def __init__(self, spark):
        self.spark = spark
    
    def execute(self):
        try:
            benefits = self.spark.read.parquet("project/database/silver/benefits")

            companies = benefits.select("CNPJ_CPF", "VALOR_REEMBOLSADO", "TIPO_DESPESA", "ANO", "MES", "COD_DOCUMENTO")
            companies = companies.withColumn("CNPJ_CPF", f.when(f.isnan(f.col("CNPJ_CPF")), "SEM REGISTRO").otherwise(f.col("CNPJ_CPF")))
        
            companies_more_revenue_senators_and_service = companies.select("CNPJ_CPF", "VALOR_REEMBOLSADO", "TIPO_DESPESA")\
                    .groupBy("CNPJ_CPF", "TIPO_DESPESA")\
                    .agg(f.sum("VALOR_REEMBOLSADO").alias("VALOR_REEMBOLSADO"))\
                    .orderBy("VALOR_REEMBOLSADO", ascending=False)
            
            companies_more_revenue_senators_and_service.withColumn("VALOR_REEMBOLSADO", f.col("VALOR_REEMBOLSADO").cast(DecimalType(25,2)))
            logging.info(companies_more_revenue_senators_and_service.show())

            companies_more_revenue_senators_and_service.repartition(1).write.mode("overwrite")\
                .parquet("project/database/gold/companies_more_revenue_senators")

            return generateResponse(msg = "Companies more revenue wrote in gold layer")

        except Exception as exp:
            logging.error(f"ERROR: {exp}")

            return generateResponse("ERROR", str(exp))