from pyspark.sql import SparkSession


def perform_sql_queries(submissions_df, comments_df, spark):
    # Register DataFrames as SQL temporary views
    submissions_df.createOrReplaceTempView("submissions")
    comments_df.createOrReplaceTempView("comments")

    # Example SQL query
    # Modify queries according to your analytical needs
    submissions_author = spark.sql(
        """
        SELECT Author
        FROM submissions
    """
    ).show()

    comments_author = spark.sql(
        """
        SELECT Author
        FROM comments
    """
    ).show()
    # Add more queries as needed
