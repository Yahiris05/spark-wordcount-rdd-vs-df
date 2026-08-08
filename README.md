# spark-wordcount-rdd-vs-df

# Descripción General

Este proyecto implementa un pipeline de procesamiento de datos utilizando PySpark para comparar el rendimiento entre dos estructuras fundamentales de Apache Spark: RDDs (Resilient Distributed Datasets) y DataFrames.La evaluación se realiza mediante la ejecución del clásico algoritmo de Word Count sobre un dataset de texto sintético de aproximadamente 100 MB. 

El objetivo principal es demostrar empíricamente las ventajas de optimización que ofrecen los DataFrames (a través del Optimizador Catalyst y el motor Tungsten) frente al "overhead" de serialización que sufren los RDDs al operar en Python.1. Requisitos Previos e InfraestructuraEl proyecto está diseñado para ejecutarse en un entorno local o simulado en la nube.

Entorno recomendado: GitHub Codespaces (o cualquier entorno Linux/macOS con Bash).
Lenguaje: Python 3.8 o superior.
Dependencias principales:pyspark (v4.2.0 o compatible)pandas (v3.0.5 o compatible, utilizado para utilidades subyacentes)
java (JRE/JDK 8 u 11, requerido por Spark)

# 2. Instalación y Configuración del EntornoSigue estos pasos para configurar el entorno desde cero en la terminal:

Paso 1: Instalar dependencias de Pythonpip install pyspark pandas
Paso 2: Generar el Dataset de PruebaPara evitar almacenar archivos pesados en el repositorio, el dataset de 100 MB se genera de forma dinámica descargando un archivo base y multiplicándolo:# 1. Descargar archivo base de texto (~6MB)
wget -O texto_base.txt https://www.norvig.com/big.txt

# 2. Multiplicar el archivo 16 veces para alcanzar ~100MB
for i in {1..16}; do cat texto_base.txt >> dataset_100MB.txt; done

# 3. Eliminar el archivo base temporal
rm texto_base.txt

# 3. Estructura del Proyectoword_count.py: 
Script principal de PySpark que contiene la lógica para ambos métodos (RDD y DataFrame).dataset_100MB.txt: Archivo de datos generado localmente (ignorado en Git)..gitignore:
Archivo de configuración para excluir datos pesados y temporales del control de versiones.output_rdd/: Directorio generado automáticamente con los resultados del Word Count usando RDD.output_df/: Directorio generado automáticamente con los resultados del Word Count usando DataFrame (formato CSV).

# 4. Ejecución del Código
Para ejecutar el pipeline de procesamiento, asegúrate de estar en el directorio raíz del proyecto y ejecuta:
python word_count.py
El script inicializará una sesión local de Spark, ejecutará el conteo de palabras utilizando RDD, luego repetirá la operación utilizando la API de DataFrames, y finalmente imprimirá un resumen de los tiempos de ejecución en la consola.

# 5. Explicación de la Lógica del Código
*Enfoque RDD (Programación Funcional)*

Se basa en transformaciones y acciones de bajo nivel. El texto se carga y procesa de la siguiente manera:

flatMap(): Divide cada línea en palabras individuales.
filter(): Elimina espacios en blanco o cadenas vacías.
map(): Transforma cada palabra en un par clave-valor.
reduceByKey(): Agrupa por clave y suma los valores para obtener el conteo total.

*Enfoque DataFrame (Programación Declarativa)*
Se basa en una estructura tabular fuertemente tipada, aprovechando el optimizador Catalyst de Spark:
select(), explode(), split(): Separa las oraciones en palabras individuales de forma distribuida, nombrando la nueva columna como "word".
filter(): Filtra resultados nulos o vacíos.groupBy("word").
count(): Realiza la agregación del conteo de manera altamente optimizada.
orderBy(): Ordena los resultados de mayor a menor frecuencia.

# 6. Resultados y Rendimiento
Tras la ejecución en un entorno estándar, se obtuvieron los siguientes tiempos de procesamiento para el dataset de 100 MB:Tiempo RDD: 24.55 segundosTiempo DataFrame: 17.55 segundos.

Conclusión Técnica: El uso de DataFrames resultó ser un 40% más rápido. Esto demuestra el impacto negativo de la serialización al intercambiar objetos genéricos de Python con la JVM (Java Virtual Machine) en los RDDs, y valida la eficiencia del motor de ejecución Tungsten y el Optimizador Catalyst al trabajar con DataFrames.
