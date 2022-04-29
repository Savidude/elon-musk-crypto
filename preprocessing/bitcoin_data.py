from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

spark_session = SparkSession.builder \
        .master("local") \
        .appName("Elon Musk Crypto") \
        .getOrCreate()

bitcoin_data = spark_session.read.csv("../data/bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv",
                                 header=True, inferSchema=True)
bitcoin = bitcoin_data.toPandas()

# plt.figure(figsize = (25, 11))
sns.heatmap(bitcoin.isna().values[::10], cmap = ["#64b5f6", "#e57373"], xticklabels=bitcoin.columns)
plt.title("Bitcoin data missing vas", size=20)
plt.show()

"""
Visualise the large amount of missing values in bitcoin the dataset
"""
