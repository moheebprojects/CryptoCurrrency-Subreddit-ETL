from prefect import flow
from crypto_scrape4 import *
from output_manager import *
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from textblob import TextBlob
from crypto_sql2 import perform_sql_queries


# Initialize Standard Spark Session
spark = SparkSession.builder.appName("RedditSentimentAnalysis").getOrCreate()


# Read Data
# Define schema for your data (adjust according to your data structure)
submissions_schema = StructType(
    [
        StructField("Submission ID", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Author", StringType(), True),
        StructField("Selftext", StringType(), True),
        StructField("Ups", IntegerType(), True),
        StructField("Downs", IntegerType(), True),
        StructField("Created UTC", TimestampType(), True),
        StructField("URL", StringType(), True),
    ]
)

comments_schema = StructType(
    [
        StructField("Submission ID", StringType(), True),
        StructField("Comment ID", StringType(), True),
        StructField("Author", StringType(), True),
        StructField("Comment", StringType(), True),
        StructField("Ups", IntegerType(), True),
        StructField("Downs", IntegerType(), True),
        StructField("Created UTC", TimestampType(), True),
        StructField("URL", StringType(), True),
    ]
)

# Assuming `submissions_list` and `comments_list` are your scraped data
submissions_rdd = spark.sparkContext.parallelize(submissions_list)
comments_rdd = spark.sparkContext.parallelize(comments_list)

submissions_df = spark.createDataFrame(submissions_rdd, submissions_schema)
comments_df = spark.createDataFrame(comments_rdd, comments_schema)


def sentiment_analysis(text):
    return TextBlob(text).sentiment.polarity


def sentiment_analysis(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "positive"
    elif polarity < -0.1:
        return "negative"
    else:
        return "neutral"


sentiment_udf = udf(sentiment_analysis, StringType())

submissions_df = submissions_df.withColumn("sentiment", sentiment_udf(col("Selftext")))
comments_df = comments_df.withColumn("sentiment", sentiment_udf(col("Comment")))

# Using parquet (open source) for columnar storage file format.

submissions_df.write.parquet(
    "/Users/moheeb/Documents/Personal_Project_ideas/crypto_data_project/submissions.parquet",
    mode="overwrite",
)
comments_df.write.parquet(
    "/Users/moheeb/Documents/Personal_Project_ideas/crypto_data_project/comments.parquet",
    mode="overwrite",
)

#########################################################################################
submissions_df = spark.read.parquet(
    "/Users/moheeb/Documents/Personal_Project_ideas/crypto_data_project/submissions.parquet"
)
comments_df = spark.read.parquet(
    "/Users/moheeb/Documents/Personal_Project_ideas/crypto_data_project/comments.parquet"
)


print(submissions_df)
submissions_df.show()
print(comments_df)
comments_df.show()
submissions_df.select("Submission ID", "Author", "sentiment").show()
comments_df.select("Comment ID", "Author", "sentiment").show()


#########################################################################################


if __name__ == "__main__":
    # The logic for processing data with spark
    perform_sql_queries(submissions_df, comments_df, spark)
