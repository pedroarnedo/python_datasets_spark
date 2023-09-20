from pyspark.sql import SparkSession

# Crear una sesión Spark
spark = SparkSession.builder.appName("EjemploCienciaDeDatos").getOrCreate()

# Leer un conjunto de datos (reemplaza 'tu_dataset.csv' con la ubicación de tu archivo CSV)
df = spark.read.csv('tu_dataset.csv', header=True, inferSchema=True)

# Mostrar las primeras filas del DataFrame
df.show()

# Resumen estadístico del conjunto de datos
df.describe().show()

# Seleccionar una columna específica
columna_seleccionada = df.select('nombre_de_la_columna')

# Filtrar filas basadas en una condición
filtro = df.filter(df['nombre_de_la_columna'] > 50)

# Agrupar y resumir datos
agrupado = df.groupBy('columna_de_agrupación').agg({'columna_de_interés': 'mean'})

# Visualización básica de datos (requiere Matplotlib)
import matplotlib.pyplot as plt
import pandas as pd

# Convertir el DataFrame de PySpark a Pandas para visualización
pandas_df = df.toPandas()

# Histograma de una columna
plt.hist(pandas_df['nombre_de_la_columna'], bins=10)
plt.xlabel('Etiqueta X')
plt.ylabel('Etiqueta Y')
plt.title('Histograma')
plt.show()

# Cerrar la sesión Spark al finalizar
spark.stop()
