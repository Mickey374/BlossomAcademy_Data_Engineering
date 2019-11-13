#!/usr/bin/env python
# coding: utf-8

# In[2]:


#Importing libraries
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import re
import datetime
import numpy as np
import pandas as pd


# url = "https://meqasa.com/properties-for-rent-in-ghana"
# html = urlopen(url)

# In[3]:


results = requests.get("https://meqasa.com/properties-for-rent-in-ghana")


# In[4]:


results.status_code


# In[5]:


results.headers


# In[6]:


results.content


# In[7]:


soup = BeautifulSoup(results.content, 'lxml')


# In[8]:


soup


# In[9]:


soup.find_all('h2')


# In[10]:


#Creating an Empty List for the Title and appending all titles
title = []

this_title = soup.find_all('h2')

for tag in this_title:
    title.append(tag.text.replace('\n',''))


# In[11]:


title


# In[12]:


#Creating an Empty List for the Description and appending all description
description = []
desc = soup.find_all('div', {'class': 'mqs-prop-dt-wrapper'})

for de in desc:
    description.append(de.find_all('p')[1].text)


# In[13]:


description


# In[63]:


combined = []
desc_1 = soup.find_all('div', {'class': 'mqs-prop-dt-wrapper'})

for co in desc_1:
    combined.append(split(co.find_all('p')[0].text.split('/')[0]))


# In[22]:


combined


# In[64]:



currency = [row[1] for row in combined]


# In[75]:


currency


# In[66]:


price = [row[0] for row in combined]


# In[76]:


price


# In[27]:


rent_period = []

for this in this_price:
    rent_period.append(this_price[0].find_all('p')[0].text.split("\n")[1].split("/")[1])


# In[28]:


rent_period


# In[33]:


#Creating an Empty list for all the price to append it.
price = []
original_price=[]
currency = []
period = []

this_price = soup.find_all('div', {'class': 'mqs-prop-dt-wrapper'})

for prop_price in this_price:
    price.append(prop_price.text.replace('\nPrice',''))


# In[16]:


current = this_price[0].find_all('p')[0].text.split("\n")[1].split("/")[0].split('Price')[1]


# In[17]:


this_price[0].find_all('p')[0].text.split("\n")[1].split("/")[0].split('Price')[1]


# In[18]:


this_price[0].find_all('p')[0].text.split("\n")[1].split("/")[1]


# In[19]:


#A function to Split into Currency, Price, Period

def split (toSplit):
    toSplit = toSplit.replace(' ', '').replace('\n', '')
    currency = '$' if ("$" in toSplit) else "GH₵"
    words = toSplit.split(currency)[1:]
    words.append(currency)
    
    if len(words) ==1:
        words = [None, None]
    return words


# In[20]:


split(current)


# In[40]:


if "GH₵" in ori_price[0]:
    currency = "GH₵"
else:
    currency = '$'

amount = ori_price[0].replace(currency, '')


# In[48]:


re.sub(r'[0-9]+', '', ori_price[0])


# In[29]:


soup.find_all('p', {'class': 'h3'})


# In[30]:


soup.find_all('li')


# In[54]:


all_bed = soup.find_all('li', {'class': 'bed'})


# In[55]:


bed = []

for this in all_bed:
    bed.append(this.text)

#soup.find_all('li', {'class': 'bed'}


# In[56]:


bed


# In[57]:


all_bed


# In[35]:


date_post = soup.find_all('p', {'class': 'wsnr'})


# In[36]:


date_posted = []

for date in date_post:
    date_posted.append(date.text)


# In[37]:


date_posted


# In[38]:


all_shower = soup.find_all('li', {'class': 'shower'})


# In[39]:


shower = []

for show in all_shower:
    shower.append(show.text)


# In[40]:


shower


# In[41]:


all_area = soup.find_all('li', {'class': 'area'})


# In[42]:


all_area


# In[43]:


area = []

for this in all_area:
    area.append(this.text)


# In[44]:


area


# In[45]:


all_garage = soup.find_all('li', {'class': 'garage'})


# In[46]:


garage = []

for gar in all_garage:
    garage.append(gar.text)


# In[47]:


garage


# In[48]:


address = soup.find_all('span', {'class': 'profile_cover_address'})


# In[49]:


location = []

for add in address:
    location.append(add.text)


# In[50]:


location


# In[77]:


#Converting all list into a DataFrame
data ={'Property': title,
     'Beds': bed,
     'Showers': shower,
     'Garages': garage,
     'Area': area,
     'Description': description,
     'Price': price,
     'Currency': currency,
     'Rent_Period': rent_period,
     'Url': title,
     'Address': location,
     'Time_Posted': date_posted
        }


# In[78]:


#Padding empty lists in the Arrays to convert into a Dataframe
max_n = max([len(x) for x in data.values()])
for field in data:
    data[field] += [''] * (max_n - len(data[field]))


# In[79]:


data


# In[80]:


#Converting to a DataFrame
df = pd.DataFrame(data)


# In[81]:


df


# In[82]:


df.to_csv(r'C:/Users/USER/Desktop/Blossom_Academy/meqasa.csv')


# In[ ]:




