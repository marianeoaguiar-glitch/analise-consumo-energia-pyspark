# =====================================================================
# PROJETO: Análise e Processamento de Dados de Consumo Elétrico
# AUTORA: Mariane Aguiar
# TECNOLOGIAS: Python, PySpark, Pandas
# =====================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, round, when, hour

# 1. INICIALIZAÇÃO DA SESSÃO SPARK
spark = SparkSession.builder \
    .appName("AnaliseConsumoEnergia") \
    .getOrCreate()

print("Sessão PySpark iniciada com sucesso!")

# 2. CRIAÇÃO DE DATAFRAME SIMULADO DE MEDIÇÃO ELÉTRICA (SMART METERS)
data = [
    ("MED-001", "2024-10-01 08:00:00", 12.5, 220.0, 0.95),
    ("MED-001", "2024-10-01 18:30:00", 28.4, 215.0, 0.88),  # Horário de Pico
    ("MED-002", "2024-10-01 09:00:00", 8.2,  222.0, 0.98),
    ("MED-002", "2024-10-01 19:00:00", 19.1, 212.0, 0.82),  # Baixo Fator de Potência
    ("MED-003", "2024-10-01 10:00:00", 45.0, 218.0, 0.92),
    ("MED-003", "2024-10-01 18:00:00", 52.3, 210.0, 0.85)   # Horário de Pico
]

columns = ["medidor_id", "timestamp", "consumo_kwh", "tensao_volts", "fator_potencia"]

df = spark.createDataFrame(data, schema=columns)

# 3. TRANSFORMAÇÃO E LIMPEZA DE DADOS
# Identificando horário de pico (entre 18h e 21h) e alertas de fator de potência (< 0.92)
df_analise = df.withColumn("timestamp_parsed", col("timestamp").cast("timestamp")) \
               .withColumn("hora", hour(col("timestamp_parsed"))) \
               .withColumn("horario_pico", when((col("hora") >= 18) & (col("hora") <= 21), "SIM").otherwise("NÃO")) \
               .withColumn("alerta_fator_potencia", when(col("fator_potencia") < 0.92, "REATIVO_EXCESSIVO").otherwise("NORMAL"))

print("\n--- Tabela Processada no PySpark ---")
df_analise.show()

# 4. AGREGATÍVOS E MÉTRICAS POR MEDIDOR
df_metricas = df_analise.groupBy("medidor_id").agg(
    round(avg("consumo_kwh"), 2).alias("consumo_medio_kwh"),
    round(max("consumo_kwh"), 2).alias("consumo_maximo_kwh"),
    round(avg("tensao_volts"), 1).alias("tensao_media_v"),
    round(avg("fator_potencia"), 2).alias("fp_medio")
)

print("\n--- Consolidação de Métricas por Unidade ---")
df_metricas.show()

# Finaliza a sessão Spark
spark.stop()
