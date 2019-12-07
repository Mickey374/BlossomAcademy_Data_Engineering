from datetime import datetime
import time

import boto3
import pandas as pd

from looper_scraper import scrape_looper
from meq_scraper import scrape_meqasa


# TODO: implement a cache to avoid pushing the same listing more than once
# cahce[listing_id] = records
cache = dict()

# initialize a kinesis client with boto3
kinesis = boto3.client('kinesis', region_name='us-east-2')


def get_data(criteria):
    columns = ['address', 'beds', 'showers', 'area', 'price', 'currency', 'url', 'source']
    data1 = scrape_meqasa(1)  # ignore the argument 1
    data2 = scrape_looper(1)
    
    data1 = data1.head(3)[columns]
    data2 = data2.head(3)[columns] # our assumption kicks in here, first 2 listings are new
    
    
    data = pd.concat([data1, data2]).reset_index().drop(columns=['index'])
    # we may have some criteria to filter our data on eg "address='adenta'"
    if criteria:
        data = data.query(criteria)
    
    return data


def listings_producer(stream_name, data):
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=data,
            PartitionKey='blossom',
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Records successfully pushed to {stream_name} within kinesis")
        else:
            print("Records not placed in kinesis")


if __name__ == '__main__':
    try:
        while True:
            data = get_data(None)
            data = data.to_json()
            listings_producer('real_estate', data)
            time.sleep(10)
            
    except Exception as e:
        print(f"Writing failed. Exiting gracefully due to {e}")
