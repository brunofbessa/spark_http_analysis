Analise de logs HTTP com PySpark

Autor: Bruno F. Bessa

%spark.pyspark
# Importacao de pacotes necessarios:

import pyspark.sql.functions
from pyspark.sql.types import *

# Carga dos arquivos de requisicoes HTTP via Spark Context na forma de RDD.

textFile1 = sc.textFile("NASA_access_log_Jul95")
textFile2 = sc.textFile("NASA_access_log_Ago95")

logLines = textFile1.union(textFile2)
logLines.cache()

# A descricao dos registros dos logs e a seguinte:

# host responsavel pela requisicao
# timestamo da data
# requisicao
# codigo de retorno HTTP
# total de bytes retornados
# Com estas informacoes, farei a quebra em colunas do arquivo de texto de uma forma mais 
# intuitiva do que utilizando expressoes regulares. Vemos que o formato padrao dos registros segue a forma abaixo:

# ‘199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] “GET /history/apollo/ HTTP/1.0” 200 6245’

# Entao usarei alguns dos caracteres que delimitam os campos para separa-los.

# Identificando alguns caracteres que podem ser substituidos por um unico identificador.
temp_var = logLines.map(lambda k: k.replace(" - - [", ";"))
temp_var2 = temp_var.map(lambda k: k.replace('] "', ";"))
temp_var3 = temp_var2.map(lambda k: k.replace('" ', ";"))

# Os dois ultimos campos podem ser separados posteriormente pois têáem o formato mais simples. 
# Com o caracter ; podemos separar as variaveis e criar um data frame.
temp_var4 = temp_var3.map(lambda k: k.split(";"))
logLinesDF = temp_var4.toDF()

# Tratando a separacao das duas ultimas colunas:
split_col = pyspark.sql.functions.split(logLinesDF["_4"], " ")
logLinesDF = logLinesDF.withColumn("codigo_http", split_col.getItem(0))
logLinesDF = logLinesDF.withColumn("total_bytes", split_col.getItem(1))

# Da mesma forma, removerei o timezone da data-hora:
split_col = pyspark.sql.functions.split(logLinesDF["_2"], " -")
logLinesDF = logLinesDF.withColumn("data_hora_string", split_col.getItem(0))
logLinesDF = logLinesDF.withColumn("timezone", split_col.getItem(1))

# Renomeando as colunas do dataframe
logLinesDF = logLinesDF.select(pyspark.sql.functions.col("_1").alias("host"), 
                                pyspark.sql.functions.col("data_hora_string").substr(1, 11).alias("data_string"),
                                pyspark.sql.functions.col("_3").alias("requisicao"),
                                pyspark.sql.functions.col("codigo_http").alias("codigo_http"),
                                pyspark.sql.functions.col("codigo_http").alias("total_bytes"))

# Resolvendo os problemas propostos no desafio:

logLinesDF.select("host").distinct().count()
