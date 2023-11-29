# CryptoCurrrency-Subreddit-ETL

# Project Overview
Purpose:
The project aims to analyze the CryptoCurrency subreddit community sentiment and discussions around the CryptoCurrency overall market. The goal is to extract posts and comments from the CryptoCurrency subreddit, apply sentiment analysis, and organize the data for insights and visualization.

## Key Objectives:

Extract data from the CryptoCurrency subreddit to analyze community sentiment.
Determine patterns and trends in discussions related to cryptocurrency.
Understand the correlation between post engagement (upvotes, comments) and sentiment.

## Technology Stack:

PRAW API: To scrape posts and comments from the CryptoCurrency subreddit community.
Pandas: For data manipulation and transformation of the scraped data.
PySpark: To handle large datasets and perform data processing and sentiment analysis.
TextBlob: An NLP library used for calculating sentiment scores for both posts and comments.
SQL Queries in PySpark: For data querying and aggregation from the processed datasets.
Local Storage: Storing processed data in Parquet format for efficient retrieval and analysis.

# Pipeline Architecture

### Data Extraction:

Utilize PRAW API to fetch posts and comments from the CryptoCurrency subreddit.
Extract relevant data such as post IDs, titles, authors, comments, upvotes, downvotes, timestamps, and URLs.

### Data Processing:

Convert the raw data into structured formats (DataFrames) using Pandas.
Transition from Pandas DataFrames to PySpark DataFrames for more robust processing.
Define schemas for submissions and comments to structure the data appropriately in PySpark.

### Sentiment Analysis:

Implement sentiment analysis on text data (posts and comments) using the TextBlob library.
Categorize sentiment as positive, negative, or neutral based on the polarity score.

### Data Storage:

Store the processed data with sentiment analysis into Parquet files for efficient storage and retrieval.

### Data Querying and Aggregation:

Utilize PySpark SQL queries to extract insights from the data.
Perform SQL operations to identify key authors, engagement metrics, and sentiment distributions.
Insights and Visualization
The project is set up to enable the creation of visualizations and dashboards based on the processed and queried data. (Note: Specific details on visualization tools or dashboards were not provided in the code.)

# Further Development
The SQL queries can be expanded and modified for more detailed insights.
Integration with visualization tools like Looker Studio, Tableau, or a custom dashboard can be implemented for better data representation.
The pipeline can be enhanced with additional features such as data profiling, monitoring, and CI/CD tooling for automation.
This summary outlines the general structure and objectives of your project, detailing the technological components and the steps taken for data processing and analysis.
