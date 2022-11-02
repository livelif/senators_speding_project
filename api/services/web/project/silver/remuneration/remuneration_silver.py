from pyspark.sql import functions as f
from ...utils.reponse_generator import generateResponse

import logging

class Remuneration():
    def __init__(self, spark):
        self.spark = spark
    

    def execute(self):
        try:
            output_df = self.spark.read.csv("project/database/bronze/remuneration", 
                    encoding="ISO-8859-1", header=True, comment= "Ú", sep=";", 
                    ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True
                    )
            output_df = output_df.withColumn("FILE_YEAR", f.lit(2022))
            output_df = self._selectColumnsAndRename(output_df)

            # Cast to double
            cols_to_double = ["REMUN_BASICA", 
                "VANT_PESSOAIS", 
                "FUNC_COMISSIONADA", 
                "GRAT_NATALINA", 
                "HORAS_EXTRAS", 
                "OUTRAS_EVENTUAIS", 
                "ABONO_PERMANENCIA", 
                "REVERSAO_TETO_CONST", 
                "IMPOSTO_RENDA", 
                "PREVIDENCIA", 
                "FALTAS", 
                "REM_LIQUIDA", 
                "DIARIAS", 
                "AUXILIOS", 
                "VANT_INDENIZATORIAS"]
            output_df = self._castToDouble(cols_to_double, output_df)

            logging.info(f"SCHEMA: {output_df.printSchema()}")

            self._writeInFileSystem(output_df)

            return generateResponse(msg="Remuneration wrote in silver layer")

        except Exception as exp:
            logging.error(f"ERROR: {exp}")

            return generateResponse("ERROR", str(exp))


    def _selectColumnsAndRename(self, df):
        output_df_columns_renamed = df.select(f.col("VÍNCULO").alias("VINCULO"),
                "CATEGORIA", 
                 "CARGO", 
                 f.col("REFERÊNCIA CARGO").alias("REFERENCIA_CARGO"),
                f.col("SÍMBOLO FUNÇÃO").alias("SIMBOLO_FUNCAO"),
                f.col("ANO EXERCÍCIO").alias("ANO_EXERCICIO"),
                f.col("LOTAÇÃO EXERCÍCIO").alias("LOTACAO_EXERCICIO"),
                f.col("TIPO FOLHA").alias("TIPO_FOLHA"),
                f.col("REMUN_BASICA"),
                f.col("VANT_PESSOAIS"),
                f.col("FUNC_COMISSIONADA"),
                f.col("GRAT_NATALINA"),
                f.col("HORAS_EXTRAS"),
                f.col("OUTRAS_EVENTUAIS"),
                f.col("ABONO_PERMANENCIA"),
                f.col("REVERSAO_TETO_CONST"),
                f.col("IMPOSTO_RENDA"),
                f.col("PREVIDENCIA"),
                f.col("FALTAS"),
                f.col("REM_LIQUIDA"),
                f.col("DIARIAS"),
                f.col("AUXILIOS"),
                f.col("VANT_INDENIZATORIAS"),
                 f.col("FILE_YEAR"))
        return output_df_columns_renamed
    
    def _castToDouble(self, cols, df):
        for col in cols:
            df = df.withColumn(col, f.regexp_replace(col, ",", ".").cast("double"))
        
        return df
    
    def _writeInFileSystem(self, df):
        df.repartition(1).write.mode('overwrite').parquet("project/database/silver/remuneration")