from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Crear una sesión Spark
spark = SparkSession.builder.appName("EjemploCienciaDeDatosAvanzado").getOrCreate()

# Leer un conjunto de datos (reemplaza 'tu_dataset.csv' con la ubicación de tu archivo CSV)
df = spark.read.csv('tu_dataset.csv', header=True, inferSchema=True)

# Realizar operaciones más avanzadas:
# Ejemplo 1: Agrupar por una columna y calcular la media de otra
df_grouped = df.groupBy('columna_de_agrupación').agg({'columna_de_interés': 'mean'})

# Ejemplo 2: Filtrar datos basados en múltiples condiciones
df_filtered = df.filter((col('columna1') > 50) & (col('columna2') < 30))

# Ejemplo 3: Realizar una unión (join) con otro DataFrame
df2 = spark.read.csv('otro_dataset.csv', header=True, inferSchema=True)
df_joined = df.join(df2, on='clave_común', how='inner')

# Visualización avanzada (requiere Matplotlib o Seaborn)
# Ejemplo: Gráfico de dispersión (scatter plot) entre dos columnas
scatter_data = df_filtered.select('columna1', 'columna2').toPandas()
plt.scatter(scatter_data['columna1'], scatter_data['columna2'])
plt.xlabel('Columna 1')
plt.ylabel('Columna 2')
plt.title('Gráfico de Dispersión')
plt.show()

# Cerrar la sesión Spark al finalizar
spark.stop()
