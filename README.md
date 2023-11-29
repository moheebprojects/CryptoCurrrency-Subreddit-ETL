# CryptoCurrrency-Subreddit-ETL

# Project Overview
## Purpose:
This project is designed to capture and analyze online discussions about cryptocurrency, in the CryptoCurrency subreddit. It aims to extract valuable insights from both submissions (original posts) and comments to gauge community sentiment, identify influential discussions, and understand engagement patterns within the cryptocurrency subreddit.

## Key Questions:

What are the prevailing sentiments in both submissions and comments in the cryptocurrency subreddit?
Which submissions are attracting the most engagement in terms of comments and upvotes?
How does the sentiment expressed in comments correlate with that in the submissions?
What topics or submissions are driving the most active discussions?

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

Utilize PySpark SQL queries to extract insights from the data.
Perform SQL operations to identify key authors, engagement metrics, and sentiment distributions.
Insights and Visualization
The project is set up to enable the creation of visualizations and dashboards based on the processed and queried data. (Note: Specific details on visualization tools or dashboards were not provided in the code.)

# Further Development
The SQL queries can be expanded and modified for more detailed insights.
Integration with visualization tools like Looker Studio, Tableau, or a custom dashboard can be implemented for better data representation.
The pipeline can be enhanced with additional features such as data profiling, monitoring, and CI/CD tooling for automation.
This summary outlines the general structure and objectives of your project, detailing the technological components and the steps taken for data processing and analysis.
