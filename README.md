# senators_speding_project

Hello guys!

Last week was the election in Brazil and if you don't know Lula won! Furthermore, I remember a project that I created to analyze how much we spent with senators in Brazil and other insights.

You can create your analysis if you want using this website: https://www12.senado.leg.br/dados-abertos 

There are some insights that I had:

### How much the senators spent with benefits paid for by the government in BRL

Legend:
NomeCompletoParlamentar = Name of Senator
ANO = Year
Valor_Reembolsado = How much is the senator spending in BRL

|NomeCompletoParlamentar |ANO |VALOR_REEMBOLSADO | 
|--------------------------------|----|------------------| 
|David Samuel Alcolumbre Tobelem |2020|513609.0 |
|David Samuel Alcolumbre Tobelem |2021|508046.74 |
|David Samuel Alcolumbre Tobelem |2016|472266.99000000005|
|Sérgio de Oliveira Cunha |2014|464327.38000000006|
|Sérgio de Oliveira Cunha |2013|464189.94999999995|

### Found the company that had more revenue for the senators in BRL

Legend:
CNPJ or CPF represents the ID for this company
Tipo de despesa = Kind of spending
ANO = Year
VALOR_REEMBOLSADO = Spending

|CNPJ_CPF |TIPO_DESPESA |ANO |VALOR_REEMBOLSADO| 
|------------------|--------------------------------------------------------------------------------------------------------------------------------------|----|-----------------|
|29.337.655/0001-31|Divulgação da atividade parlamentar |2017|270000.0 | 
|04.953.008/0001-23|Divulgação da atividade parlamentar |2016|256980.0 | 
|38.006.631/0001-90|Divulgação da atividade parlamentar |2015|216780.0 | 
|07.053.387/0001-93|Divulgação da atividade parlamentar |2014|162400.0 | 
|01.736.567/0001-93|Contratação de consultorias, assessorias, pesquisas, trabalhos técnicos e outros serviços de apoio ao exercício do mandato parlamentar|2014|150000.0 | 

###  How many employees does each senator have

Legend:
Senador = Senator

| SENADOR|TOTAL| 
|--------------------|-----| 
| KÁTIA ABREU| 85| 
| IZALCI LUCAS| 82| 
| ROGÉRIO CARVALHO| 73| 
| LUCAS BARRETO| 72| 
| ROBERTO ROCHA| 71|

###  How much cost the senator's employees per year

VINCULO = Kind of contract 
SENADOR = Senator
FILE_YEAR = Year

| VINCULO| SENADOR|FILE_YEAR| TOTAL|
|------------|-------------------|---------|------------------| 
|COMISSIONADO| NELSINHO TRAD| 2022|465207.24000000034| 
|COMISSIONADO| IZALCI LUCAS| 2022| 455605.4900000002| 
|COMISSIONADO| ROBERTO ROCHA| 2022|442890.07000000024| 
|COMISSIONADO| LUCAS BARRETO| 2022| 407766.0100000003| 
|COMISSIONADO|WELLINGTON FAGUNDES| 2022| 399503.8000000001| 
|COMISSIONADO| EDUARDO GOMES| 2022| 374672.4600000002|


Technologies that I used: 
Pyspark
Airflow
Flask
Python

The architecture, you can see in the Images below.

Link for the project: 