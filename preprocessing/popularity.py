from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import seaborn as sns

import matplotlib.pyplot as plt

spark_session = SparkSession.builder \
        .master("local") \
        .appName("Elon Musk Crypto") \
        .getOrCreate()
tweets = spark_session.read.csv("../data/TweetsElonMusk.csv", header=True, inferSchema=True)

def get_year(date):
        return date.split("-")[0]
udf_year = F.udf(lambda x:get_year(x),StringType())
tweets = tweets.withColumn("year", udf_year(F.col("date")))

popularity = tweets.groupby("year").agg({'likes_count' : 'sum',
                                                'retweets_count' : 'sum',
                                                'replies_count' : 'sum',
                                                'tweet' : 'count'})

popularity = popularity.withColumn("likes_per_tweet", (popularity["sum(likes_count)"] / popularity["count(tweet)"]))
popularity = popularity.withColumn("retweets_per_tweet", (popularity["sum(retweets_count)"] / popularity["count(tweet)"]))
popularity = popularity.withColumn("replies_per_tweet", (popularity["sum(replies_count)"] / popularity["count(tweet)"]))
popularity = popularity.sort(F.asc("year"))
popularity = popularity.toPandas()

fig, (ax1, ax2, ax3) = plt.subplots(3, figsize=(25, 15))
axs = [ax1, ax2, ax3]
plt.suptitle("Popularity", size=25)
sns.barplot(data=popularity, x="year", y="likes_per_tweet", lw=5, color="#e57373", ax=ax1)
sns.barplot(data=popularity, x="year", y="retweets_per_tweet", lw=5, color="#4db6ac", ax=ax2)
sns.barplot(data=popularity, x="year", y="replies_per_tweet", lw=5, color="#64b5f6", ax=ax3)
names = ["Average Likes", "Average Retweets", "Average Replies"]
for ax, n in zip(axs, names):
    ax.set_ylabel(n, size=20)
    ax.get_yaxis().set_ticks([])
sns.despine(left=True)
plt.show()
