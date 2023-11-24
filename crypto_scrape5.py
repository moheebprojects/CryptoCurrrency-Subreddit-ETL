# subreddit_data_fetch.py
import pandas as pd
import praw
import os
from pyspark.sql import SparkSession
import time
from prefect import task
from datetime import datetime
from output_manager import OutputManager
from pathlib import Path
from textblob import TextBlob
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def web_scrape_crypto_subreddit(
    client_id: str,
    client_secret: str,
    user_agent: str,
    reddit_username: str,
    password: str,
):
    # Function to web scrape cryptocurrency subreddit using PRAW
    reddit_api = praw.Reddit(
        client_id="client_id",
        client_secret="cleint_secret",
        user_agent="user_agent",
        reddit_username="username",
        password="password",
    )
    return reddit_api


output_manager = OutputManager(subreddit="CryptoCurrency", subreddit_cap=200)


def subreddit_submission_fetcher(
    reddit_api, output_manager: OutputManager, subreddit_name="CryptoCurrency", limit=8
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

    output_manager.submissions_list.extend(submission_data_list)
    return submission_data_list


def comments_fetcher(
    submission_id: str, output_manager: OutputManager, reddit_api: praw.Reddit
):
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
        output_manager.comments_list.append(comment_data)

    return comment_data_list


reddit_api = web_scrape_crypto_subreddit(
    client_id="McMt3gyqN6aLSitkzv47EQ",
    client_secret="Dy_QMRCAmpN1WOQ9SOyaN0HqkOWBEw",
    user_agent="scraperr 1.0 by /u/TheSilentData",
    reddit_username="TheSilentData",
    password="Yemen6987",
)

output_manager = OutputManager(
    "/Users/moheeb/Documents/Personal_Project_ideas/crypto_data_project",
    subreddit_cap=200,
)

# Fetch submissions
submissions_list = subreddit_submission_fetcher(reddit_api, output_manager)
comments_list = []

# Fetch comments for each submission
for submission_tuple in submissions_list:
    submission_id = submission_tuple[0]  # Extract the submission ID from the tuple
    comments_list.extend(comments_fetcher(submission_id, output_manager, reddit_api))

# Convert to pandas DataFrame
submissions_df = pd.DataFrame(
    submissions_list,
    columns=[
        "Submission ID",
        "Title",
        "Author",
        "Selftext",
        "Ups",
        "Downs",
        "Created UTC",
        "URL",
    ],
)
comments_df = pd.DataFrame(
    comments_list,
    columns=[
        "Submission ID",
        "Comment ID",
        "Author",
        "Comment",
        "Ups",
        "Downs",
        "Created UTC",
        "URL",
    ],
)
