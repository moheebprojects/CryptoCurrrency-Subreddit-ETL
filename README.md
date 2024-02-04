# CryptoCurrrency-Subreddit-ETL

# Project Overview
## Purpose:
This project is designed to capture and analyze online discussions about cryptocurrency, in the CryptoCurrency subreddit. It aims to extract valuable insights from both submissions (original posts) and comments to gauge community sentiment, identify influential discussions, and understand engagement patterns within the CryptoCurrency subreddit.

## Key Questions:

- What are the prevailing sentiments in both submissions and comments in the cryptocurrency subreddit?

- Which submissions are attracting the most engagement in terms of comments and upvotes?

- How does the sentiment expressed in comments correlate with that in the submissions?

- What topics or submissions are driving the most active discussions?

## Technology Stack:

- PRAW API: For scraping detailed data from the cryptocurrency subreddit.
  
- Pandas: For initial data processing and organization into DataFrames.
  
- PySpark: Utilized for handling larger datasets, performing sentiment analysis, and managing complex data processing tasks.

- TextBlob: An NLP library for performing sentiment analysis on the textual content of submissions and comments.

- Parquet File Format: For efficient storage and retrieval of the processed data.

## Pipeline Architecture and Data Structure

### Data Extraction:

Scrape data from the subreddit using PRAW API, focusing on two types of content: submissions and comments.
Extract key information such as IDs, titles, authors, content, upvotes, downvotes, timestamps, and URLs.

### Data Transformation and Sentiment Analysis:

Convert the raw data into Pandas DataFrames for preprocessing.
Transition to PySpark DataFrames for scalable data handling.
Apply TextBlob to perform sentiment analysis, categorizing the content into positive, negative, or neutral sentiments.

### Submissions Table:

Contains structured data about subreddit posts, including Submission_ID, Title, Author, Selftext, Up_Votes, Down_Votes, Created_UTC, URL, and the sentiment of each post.
Provides a comprehensive overview of individual posts and their reception in the subreddit.

### Comments Table:

Comprises detailed data on user comments, including Submission_ID (linking to the respective post), Comment_ID, Author, Comment content, Up_Votes, Down_Votes, Created_UTC, URL, and the sentiment of each comment.
Offers insights into community reactions and discussions pertaining to specific posts.

### Data Storage:

Processed data is stored in Parquet format, balancing efficiency in both storage and access.

## Data Querying and Aggregation:

In this project, I utilized PySpark SQL queries to analyze data extracted from the cryptocurrency subreddit. The queries were designed to provide insights into community sentiments, engagement patterns, and controversial topics. The data was obtained from the submissions posted onto the CryptoCurrency subrredit and the comments on each post submitted.


