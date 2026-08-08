from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower
import time

def main():
    # 1. Inicialización de Spark
    print("Iniciando SparkSession...")
    spark = SparkSession.builder \
        .appName("WordCount_RDD_vs_DataFrame") \
        .master("local[*]") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR") # Para evitar exceso de logs en consola

    file_path = "dataset_100MB.txt"

    # ==========================================
    # 2. Word Count con RDDs
    # ==========================================
    print("\n--- Iniciando Word Count con RDD ---")
    start_time_rdd = time.time()

    # Cargar archivo
    rdd = sc.textFile(file_path)
    
    # flatMap -> map -> reduceByKey
    counts_rdd = rdd.flatMap(lambda line: line.lower().split()) \
                    .filter(lambda word: word != "") \
                    .map(lambda word: (word, 1)) \
                    .reduceByKey(lambda a, b: a + b)
    
    # Forzar la ejecución (acción) y guardar. Usamos coalesce(1) para generar un solo archivo de salida.
    counts_rdd.coalesce(1).saveAsTextFile("output_rdd")
    
    end_time_rdd = time.time()
    rdd_duration = end_time_rdd - start_time_rdd
    print(f"Tiempo de ejecución RDD: {rdd_duration:.2f} segundos")

    # ==========================================
    # 3. Word Count con DataFrames
    # ==========================================
    print("\n--- Iniciando Word Count con DataFrame ---")
    start_time_df = time.time()

    # Leer archivo como DataFrame de una columna llamada "value"
    df = spark.read.text(file_path)

    # explode -> split -> groupBy -> count -> orderBy
    counts_df = df.select(
        explode(split(lower(col("value")), r"\s+")).alias("word")
    ).filter(col("word") != "") \
     .groupBy("word") \
     .count() \
     .orderBy(col("count").desc())

    # Forzar ejecución y guardar en formato CSV
    counts_df.coalesce(1).write.csv("output_df", header=True, mode="overwrite")

    end_time_df = time.time()
    df_duration = end_time_df - start_time_df
    print(f"Tiempo de ejecución DataFrame: {df_duration:.2f} segundos")

    # ==========================================
    # 4. Comparación de Rendimiento
    # ==========================================
    print("\n==========================================")
    print("RESUMEN DE RENDIMIENTO")
    print("==========================================")
    print(f"RDD:       {rdd_duration:.2f} s")
    print(f"DataFrame: {df_duration:.2f} s")
    
    if df_duration > 0:
        speedup = rdd_duration / df_duration
        print(f"Speedup (RDD_time / DF_time): {speedup:.2f}x")
        print(f"El método DataFrame fue {speedup:.2f} veces más rápido (o lento) que el RDD.")

    spark.stop()

if __name__ == "__main__":
    main()
    