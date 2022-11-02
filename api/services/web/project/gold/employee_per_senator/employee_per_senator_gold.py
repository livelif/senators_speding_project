from ...utils.reponse_generator import generateResponse
from pyspark.sql import functions as f

import logging

class EmployeePerSenatorGold():
    def __init__(self, spark):
        self.spark = spark
    
    def execute(self):
        try:
            benefits = self.spark.read.parquet("project/database/silver/benefits")
            employee = self.spark.read.parquet("project/database/silver/remuneration")
            senators_regex_table = benefits.select("SENADOR").drop_duplicates()

            senatorsNameList = list(senators_regex_table.toPandas()["SENADOR"].dropna())
            senatorsNameRegex = "|".join(senatorsNameList)

            employee_liquid = employee.select("VINCULO", "CATEGORIA", "CARGO", "REFERENCIA_CARGO", "SIMBOLO_FUNCAO", "ANO_EXERCICIO", "LOTACAO_EXERCICIO", "TIPO_FOLHA", "REM_LIQUIDA", "AUXILIOS", "FILE_YEAR", "ANO_EXERCICIO")

            employee_liquid = employee_liquid.withColumn("TOTAL", f.col("REM_LIQUIDA") + f.col("AUXILIOS"))
            employee_liquid = employee_liquid.withColumn("SENADOR", f.upper("LOTACAO_EXERCICIO"))
            employee_liquid = employee_liquid.withColumn("SENADOR", f.regexp_extract("SENADOR", senatorsNameRegex, 0))
            employee_liquid = employee_liquid.withColumn("SENADOR", f.when(f.col("SENADOR") == "", "SENATE SERVICES").otherwise(f.col("SENADOR")))
            employee_liquid = employee_liquid.withColumn("CARGO", f.when(f.isnull(f.col("CARGO")), "NONEXISTENT").otherwise(f.col("CARGO")))

            employee_liquid = employee_liquid.select("VINCULO", "SENADOR", "TOTAL")\
                        .where("REM_LIQUIDA > 0") \
                        .groupby("VINCULO", "SENADOR")\
                        .agg(f.count("SENADOR").alias("count"))\
                        .orderBy("count", ascending=False)
            
            logging.info(employee_liquid.show())

            employee_liquid.repartition(1).write.mode("overwrite")\
                .parquet("project/database/gold/employee_per_senator_gold")

            return generateResponse(msg="How many Employee each senator have wrote in gold layer")

        except Exception as exp:
            logging.error(f"ERROR: {exp}")
            
            return generateResponse("ERROR", str(exp))