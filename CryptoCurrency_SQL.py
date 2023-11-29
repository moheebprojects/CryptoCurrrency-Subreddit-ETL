from pyspark.sql import SparkSession


def perform_sql_queries(submissions_df, comments_df, spark):
    # Registering the DataFrames as an  SQL temporary views
    submissions_df.createOrReplaceTempView("submissions")
    comments_df.createOrReplaceTempView("comments")

    # Assessing the overall sentiment distribution in both submissions and comments
    overall_sentiment = spark.sql(
        """SELECT sentiment, COUNT(*) as count
        FROM submissions
        GROUP BY sentiment
        UNION ALL
        SELECT sentiment, COUNT(*) as count
        FROM comments
        GROUP BY sentiment;"""
    ).show()

    # Determining the average number of upvotes and downvotes for each sentiment category.
    average_votes_ratio_by_sentiment = spark.sql(
        """SELECT sentiment, AVG(Up_Votes) as avg_upvotes, AVG(Down_Votes) as avg_downvotes
        FROM submissions
        GROUP BY sentiment
        UNION ALL
        SELECT sentiment, AVG(Up_Votes) as avg_upvotes, AVG(Down_Votes) as avg_downvotes
        FROM comments
        GROUP BY sentiment;"""
    ).show()

    # Identifying the top submissions with the most engagement (upvotes, downvotes) within each sentiment category.

    submissions_engagment_by_sentiment = spark.sql(
        """SELECT *
        FROM submissions
        WHERE sentiment = 'positive' OR sentiment = 'negative' OR sentiment = 'neutral'
        ORDER BY Up_Votes DESC, Down_Votes DESC
        LIMIT 10;"""
    ).show()

    # Analysing if there is a correlation between sentiment and the total engagement (sum of upvotes and downvotes).

    sentiment_engagement_correlation = spark.sql(
        """SELECT sentiment, SUM(Up_Votes + Down_Votes) as total_engagement, AVG(Up_Votes) as avg_upvotes, AVG(Down_Votes) as avg_downvotes
        FROM submissions
        GROUP BY sentiment
        UNION ALL
        SELECT sentiment, SUM(Up_Votes + Down_Votes) as total_engagement, AVG(Up_Votes) as avg_upvotes, AVG(Down_Votes) as avg_downvotes
        FROM comments
        GROUP BY sentiment;"""
    ).show()

    # Observing how sentiments vary over time in both submissions and comments.

    sentiment_trend_timer_series = spark.sql(
        """SELECT DATE_FORMAT(Created_UTC, 'yyyy-MM') as month, sentiment, COUNT(*) as count
        FROM submissions
        GROUP BY month, sentiment
        UNION ALL
        SELECT DATE_FORMAT(Created_UTC, 'yyyy-MM') as month, sentiment, COUNT(*) as count
        FROM comments
        GROUP BY month, sentiment;"""
    ).show()

    # Identifying the most controversial submissions.

    controversial_posts = spark.sql(
        """SELECT *, (Up_Votes + Down_Votes) as total_votes
        FROM submissions
        WHERE Up_Votes > 50 AND Down_Votes > 50
        ORDER BY total_votes DESC
        LIMIT 10;"""
    ).show()
