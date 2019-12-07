
# Blossom_Academy(Data_Engineering)

## **Project2: Data Exploration using Spark**
**Purpose**
The aim of this was to investigate the top keywords companies within various cities in the US require for Data Scientists to have in their resume.

**Tools Used:**
- Jupyter Notebook
- Pandas
- Pyspark
- Amazon Simple Storage Service(S3)

1. Load the data scientist job market dataset and us stocks dataset from s3 bucket onto your computer.
2. Read the data with pyspark and read the alldata.csv from the dataset.
3. Joining 2 datasets using a specified column for the join.
4. Tokenize the dataset and write a function to generate the ngram(unigram and bigram) for the joined dataset.
5. Write another function which uses the function from (5) to create 2 spark data frames which have 3 columns in the order of frequency:
{Ngram, City, Frequency}
{Ngram, Industry, Frequency}
6. Performing other analysis on datasets using pyspark to group, count or check for duplicates.