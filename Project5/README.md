
### Project 5 :  Stream Processing

**Purpose:**
The aim of this project is to scrape data from Meqasa website, manipulate the data and save the final output.
**Tools Used:**
- Jupyter Notebook
- AWS Kinesis
- Boto3

**Task:**
1. Creating a producer to scrape data from meqasa website.
2. Pushing the scraped data into kinesis data stream every 30 seconds.
3. Implementing a consumer and a utility function to get shard Ids.
4. Enriching the data from a message bus.