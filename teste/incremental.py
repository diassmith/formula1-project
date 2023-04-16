# Databricks notebook source
# MAGIC %run "../0 - includes/functions"

# COMMAND ----------

# MAGIC %run "../0 - includes/configuration"

# COMMAND ----------

df_source = (spark.read
               .option("header", True)
               .option("inferSchema", "true")
               .option("delimiter",';')
               .csv(f"{landing_folder_path}/08/acordo.csv"))

# COMMAND ----------

display(df_source)

# COMMAND ----------

for column in df_source.columns:
    print('"'+column+'"')

# COMMAND ----------

for column in df_source.columns:
    print('df_source.'+column+' = '+'df_sink.'+column+' and')

# COMMAND ----------

df_source = df_source.select("SkAcordo","BkAcordo","SkOperacao", "IdOperacao", "IdAcordo", "IdCliente","DtReferencia","DtUltimaModificacao","DtAcordoOferta","QtParcela","NrParcelaVencimentoPendente", "QtParcelaPaga","DsStatusParcela","DtUltimoPagamento","DtVencimentoPendente","VlAcordoOferta")

# COMMAND ----------

# df_source.createOrReplaceTempView('Source01')

# COMMAND ----------

display(df_source)

# COMMAND ----------

display(df_source)

# COMMAND ----------

df_source.write.format("delta").save(f"{landing_folder_path}/08/delta/")

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

df_sink = spark.read.format("delta").load(f"{landing_folder_path}/08/delta/")

# COMMAND ----------

df_sink = spark.read.load(f"{landing_folder_path}/08/delta/")

# COMMAND ----------

df_sink.display()

# COMMAND ----------

df_sink

# COMMAND ----------

df_source.columns

# COMMAND ----------

df_sink.merge(df_source, df_source.SkAcordo == df_sink.SkAcordo).whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()


# COMMAND ----------

joined_tbl = df_source.join(df_sink, df_source.SkAcordo == df_sink.SkAcordo, "outer")


# COMMAND ----------

from pyspark.sql import SparkSession

# cria a sessão do Spark
#spark = SparkSession.builder.appName("Atualizadf_sink").getOrCreate()

# lê a tabela delta df_source
#df_source = spark.read.format("delta").load("caminho/para/df_source")

# lê a tabela delta df_sink
# df_sink = spark.read.format("delta").load("caminho/para/df_sink")

# junta as duas tabelas pelo campo SkAcordo
#join_condition = ("df_source.SkAcordo = df_sink.SkAcordo and df_source.BkAcordo = df_sink.BkAcordo and df_source.SkOperacao = df_sink.SkOperacao and df_source.IdOperacao = df_sink.IdOperacao and df_source.IdAcordo = df_sink.IdAcordo and df_source.IdCliente = df_sink.IdCliente and df_source.DtReferencia = df_sink.DtReferencia and df_source.DtUltimaModificacao = df_sink.DtUltimaModificacao and df_source.DtAcordoOferta = df_sink.DtAcordoOferta and df_source.QtParcela = df_sink.QtParcela and df_source.NrParcelaVencimentoPendente = df_sink.NrParcelaVencimentoPendente and df_source.QtParcelaPaga = df_sink.QtParcelaPaga and df_source.DsStatusParcela = df_sink.DsStatusParcela and df_source.DtUltimoPagamento = df_sink.DtUltimoPagamento and df_source.DtVencimentoPendente = df_sink.DtVencimentoPendente and df_source.VlAcordoOferta = df_sink.VlAcordoOferta")
joined_tbl = df_source.join(df_sink, df_source.SkAcordo == df_sink.SkAcordo, "outer")

# define as colunas que devem ser atualizadas na tabela df_sink
update_columns = ["BkAcordo", "SkOperacao", "IdOperacao", "IdAcordo", "IdCliente", "DtReferencia", "DtUltimaModificacao", "DtAcordoOferta", "QtParcela", "NrParcelaVencimentoPendente", "QtParcelaPaga", "DsStatusParcela", "DtUltimoPagamento", "DtVencimentoPendente", "VlAcordoOferta"]

# define a expressão de atualização
update_expression = {col: "CASE WHEN df_source.%s IS NOT NULL THEN df_source.%s ELSE df_source.%s END" % (col, col, col) for col in update_columns}

# atualiza a tabela df_sink com as informações da tabela df_source
df_sink_updated = joined_tbl.selectExpr("COALESCE(df_source.SkAcordo, df_sink.SkAcordo) AS SkAcordo", *update_expression.values()).where("df_source.SkAcordo IS NOT NULL OR df_sink.SkAcordo IS NOT NULL")

# insere na tabela df_sink as informações que não existem na tabela df_source
df_sink_inserted = joined_tbl.selectExpr("COALESCE(df_source.SkAcordo, df_sink.SkAcordo) AS SkAcordo", *update_expression.keys()).where("df_source.SkAcordo IS NULL")

# escreve as atualizações e inserções na tabela delta df_sink
df_sink_updated.write.format("delta").mode("append").save(f"{landing_folder_path}/08/delta/")
df_sink_inserted.write.format("delta").mode("append").save(f"{landing_folder_path}/08/delta/")

# encerra a sessão do Spark
#spark.stop()

