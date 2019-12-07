#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importing Libraries
import boto3
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window


# In[ ]:


#Downloading data from the s3 bucket
s3 = boto3.client('s3')
s3.download_file('blossom-data-engs', 'all-us-stocks-tickers-company-info-logos.zip', 'allus.zip')
s3.download_file('blossom-data-engs', 'data-scientist-job-market-in-the-us.zip', 'datascientists.zip')

#Unzipping files with the ZipFile library

path1 = "../Blossom_Academy/allus.zip"

with ZipFile(path1, 'r') as zip:
    zip.printdir()
    print("Extract files")
    zip.extractall()
    print("Are the files extracted")

path2 = "../Blossom_Academy/datascientists.zip"

with ZipFile(path2, 'r') as zip:
    zip.printdir()
    print("Extract files")
    zip.extractall()
    print("Are the files extracted")
# In[3]:


#Creating a SparkSession
spark = SparkSession.builder.getOrCreate()


# In[4]:


#Read the dataset from local directory
datascientists = spark.read.csv('C:/Users/USER/Desktop/Blossom_Academy/datascientists/alldata.csv', header = True, inferSchema =True)
companies = spark.read.csv('C:/Users/USER/Desktop/Blossom_Academy/companies/companies.csv', header = True, inferSchema =True)


# In[5]:


#Checking the number of columns for the companies dataset
companies.columns


# In[6]:


#Checking the number of columns for the datascientists dataset
datascientists.columns


# In[8]:


#Splitting the column location in the Datascientists dataset
datascientists.select('location', F.split(datascientists['location'], ',')[0].alias('City')).show()


# In[10]:


#Joining the two Datasets (Company & Datascientists) and renaming one of the columns as comdescription
joindata = datascientists.join(companies.withColumnRenamed('description', 'comdescription'), datascientists['company'] == companies['company name'] )


# In[11]:


#Printing the columns of the joined datasets
joindata.columns


# In[12]:


#Showing the first 2 rows of the Joined dataset
joindata.show(5)


# ### Tokenizing the data to print N-Grams(2)

# In[13]:


#Importing libraries for Tokenization and N-Grams
import nltk
from pyspark.ml.feature import Tokenizer, NGram


# In[14]:


#Creating a variable for the Tokenization and also create a new column
token = Tokenizer(inputCol = 'description', outputCol = 'tokenized')


# In[15]:


#Dropping null values and applying tokenization
joindata.drop()
token_df = token.transform(joindata)


# In[16]:


#Selecting the description and tokenized columns and displaying 5 rows of data
token_df.select('description', 'tokenized').show(5)


# In[17]:


#Creating an NGram column and assigning it to bigram
bigram = NGram(n=2, inputCol = 'tokenized', outputCol = 'NGram')
ngram = bigram.transform(token_df)


# In[18]:


#Showing the 5 rows of the ngram dataset
ngram.show(5)


# #### Performing a value count to determine the Frequency 

# In[19]:


#Importing Libraries
from pyspark.sql.functions import explode
import pyspark.sql.functions as F


# In[20]:


#Printing the Ngram column
ngram.select('Ngram').limit(1).take(1)


# In[38]:


#Creating a count and a New column
newngram = ngram.select(['NGram' , 'tokenized']).select('tokenized', F.explode('NGram').alias('NGRAM')).groupBy(['NGram', 'tokenized']).count()


# In[36]:


#Printing the first 5 rows for the new variable
newngram.groupBy('NGram')


# In[39]:


from pyspark.sql.functions import countDistinct


# In[47]:


# Change name of columns with alias
#ngram.groupBy("NGram").agg(F.count('NGram')).show()
#ngram.select(countDistinct("NGram")).show()
newngram.select('count')


# In[65]:


newngram.show()


# In[72]:


#import ipython
import nbconvert
import import_ipynb


# In[73]:


ipython nbconvert — to script Batch_Processing.ipynb


# In[ ]:




