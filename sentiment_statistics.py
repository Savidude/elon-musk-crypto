from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, col

def get_sentiment_tweet_data():
    spark_session = SparkSession.builder \
        .master("local") \
        .appName("Elon Musk Crypto") \
        .getOrCreate()

    df = spark_session.read.csv("data/derived/tweets_analysed.csv", header=True, inferSchema=True)
    return df

def get_statistics(df, feature):
    df_stats = df.select(
        mean(col(feature)).alias('mean'),
        stddev(col(feature)).alias('std')
    ).collect()

    return df_stats[0]

def print_statistics(df, feature):
    statistics = get_statistics(df, feature)
    print("---------------- {} statistics ----------------".format(feature))
    print("{}: {}".format("mean", statistics['mean']))
    print("{}: {}".format("standard deviation", statistics['std']))
    print()

if __name__ == '__main__':
    sentiment_tweet_data = get_sentiment_tweet_data()

    features = ['replies_count', 'retweets_count', 'likes_count', 'sentiment_score']
    for feature in features:
        print_statistics(sentiment_tweet_data, feature)
