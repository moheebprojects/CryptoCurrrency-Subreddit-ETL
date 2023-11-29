# subreddit_data_fetch.py
import pandas as pd
import praw
import os
from pyspark.sql import SparkSession
import time
from prefect import task
from datetime import datetime
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from textblob import TextBlob


def web_scrape_crypto_subreddit(
    client_id: str,
    client_secret: str,
    user_agent: str,
    reddit_username: str,
    password: str,
):
    # Function to web scrape cryptocurrency subreddit using PRAW
    reddit_api = praw.Reddit(
        client_id="McMt3gyqN6aLSitkzv47EQ",
        client_secret="Dy_QMRCAmpN1WOQ9SOyaN0HqkOWBEw",
        user_agent="scraperr 1.0 by /u/**************",
        reddit_username="**************",
        password="***********",
    )
    return reddit_api


def subreddit_submission_fetcher(
    reddit_api, subreddit_name="CryptoCurrency", limit=100
):
    submission_data_list = []
    for submission in reddit_api.subreddit(subreddit_name).hot(limit=limit):
        submission_data_list.append(
            (
                submission.id,
                submission.title,
                submission.author.name if submission.author else None,
                [submission.selftext.replace(",", "")],
                submission.ups,
                submission.downs,
                datetime.utcfromtimestamp(submission.created_utc),
                submission.url,
            )
        )

    return submission_data_list


def comments_fetcher(submission_id: str, reddit_api: praw.Reddit):
    try:
        submission_rich_data = reddit_api.submission(id=submission_id)
        submission_rich_data.comment_sort = (
            "new"  # You can change this to 'top', 'new', etc.
        )
        submission_rich_data.comment_limit = 200  # Set the limit for comments
        submission_rich_data.comments.replace_more(
            limit=0
        )  # Expand all 'MoreComments' objects
        comments = submission_rich_data.comments.list()
    except praw.exceptions.PRAWException as e:
        print(f"Error fetching submission {submission_id}: {str(e)}")
        return []
    except Exception as e:
        print(f"General error for submission {submission_id}: {str(e)}")
        return []

    comment_data_list = []
    for comment in comments:
        author_name = comment.author.name if comment.author else pd.NA
        comment_data = (
            submission_id,
            comment.id,
            author_name,
            [comment.body.replace(",", "")],
            comment.ups,
            comment.downs,
            datetime.utcfromtimestamp(comment.created_utc),
            submission_rich_data.url,
        )
        comment_data_list.append(comment_data)

    return comment_data_list


reddit_api = web_scrape_crypto_subreddit(
    client_id="McMt3gyqN6aLSitkzv47EQ",
    client_secret="Dy_QMRCAmpN1WOQ9SOyaN0HqkOWBEw",
    user_agent="scraperr 1.0 by /u/**************",
    reddit_username="**************",
    password="***********",
)


# Fetching submissions
submissions_list = subreddit_submission_fetcher(reddit_api)
comments_list = []

# Fetching comments for each submission
for submission_tuple in submissions_list:
    submission_id = submission_tuple[0]  # Extract the submission ID from the tuple
    comments_list.extend(comments_fetcher(submission_id, reddit_api))

# Converting to pandas DataFrame
submissions_df = pd.DataFrame(
    submissions_list,
    columns=[
        "Submission_ID",
        "Title",
        "Author",
        "Selftext",
        "Up_Votes",
        "Down_Votes",
        "Created_UTC",
        "URL",
    ],
)
comments_df = pd.DataFrame(
    comments_list,
    columns=[
        "Submission_ID",
        "Comment_ID",
        "Author",
        "Comment",
        "Up_Votes",
        "Down_Votes",
        "Created_UTC",
        "URL",
    ],
)
