import re
import time

from bs4 import BeautifulSoup
import geocoder
import requests
import pandas as pd


def extract_rate(text):
    # eg: '\nPriceGH₵2,000 / month\n' -> 'GH₵2000 / month' 
    # some listings come with a ':' eg '\nPrice:GH₵2,000 / month\n'
    clean = text.replace('\nPrice', '').replace('\n', '').replace(',', '').replace(':', '')

    # 'GH₵2000 / month' -> 'GH₵ 2000 / month'
cleaner = re.sub(r'([0-9]+)', r' \1', clean)

    # 'GH₵ 2000 / month' -> ['GH₵', '2,000', 'month']
    cleaned = cleaner.replace(' / ', ' ').split(' ')
    
    # there are cases where the prices are not disclosed
    if len(cleaned) != 3:
        cleaned = ['Undisclosed', None, None]
    
    return cleaned

def c(x):
    "If element is empty, don't call the .text method"
    if x: 
        return x.text
    else:
        return ''


def parse_single_page(page, location):
    properties = []

    # property is a python reserved word so we'll use prop
    for prop in page.find_all("div", class_="mqs-prop-dt-wrapper"):
        name = prop.h2.a.text
#         location = name.split(' at ')[1].strip().split(' in ')[0]
        location = prop.find('h2').text.replace('@', ' at ').split(' at ')[1].strip().split(' in ')[0]
        rate = prop.find_all('p')[0].text
        
        currency, price, period = extract_rate(rate)

        # description = prop.find_all('p')[1].text
        # beds = prop.find('li', {'class': 'bed'}).text
        # showers = prop.find('li', {'class': 'shower'}).text
        # garages = prop.find('li', {'class': 'garages'}).text
        
        # This looks cryptic, but it's a way to handle pages that 
        # have missing info.

        description = c(prop.find_all('p')[1])
        beds = c(prop.find('li', {'class': 'bed'}))
        showers = c(prop.find('li', {'class': 'shower'}))
        garages = c(prop.find('li', {'class': 'garage'}))
        area = c(prop.find('li', {'class': 'area'})).replace(' m2', '').replace(',', '')
        url = prop.find('a').attrs['href']
        
        x = url.replace('/', '').split('-')
        apartment_type = x[0]
        rent_type = x[2]
        
        
        properties.append(
            {
                'property': name,
                'beds': beds,
                'showers': showers,
                'garages': garages,
                'area': area,
                'description': description,
                'price': price,
                'currency': currency,
                'rent_period': period,
                'url': url,
                'address': location,
                'apartment_type': apartment_type,
                'rent_type': rent_type,
                'time_posted': None
            }
        )

    return properties


def scrape_location(max_num_pages, location):
    
    out = []
    for page in list(range(1, max_num_pages+1)):
        print(f"Scraping page: {page} from Meqasa")
        src = requests.get(f'https://meqasa.com/houses-for-rent-in-{location}?w={page}').text 
        listings = BeautifulSoup(src,'lxml')
        out += parse_single_page(listings, location)
        
        time.sleep(1)
        
    return out


def to_num(series):
    return pd.to_numeric(series.replace({'': pd.np.NaN}), downcast='unsigned')


def preprocess(df):
    df['beds'] = to_num(df['beds'])
    df['showers'] = to_num(df['showers'])
    df['garages'] = to_num(df['garages'])
    df['rent_period'] = df['rent_period'].replace({'': pd.np.NaN, 'day': 'daily', 'week': 'weekly', 'month': 'monthly'})
    df['price'] = to_num(df['price'])
    df['currency'] = df['currency'].replace({None, pd.np.NaN})
    df['area'] = to_num(df['area'])
    df['source'] = 'meqasa'

    return df



def scrape_meqasa(max_num_pages=3, locations=['ghana']):
    listings = []

    for location in locations:
        listings += scrape_location(max_num_pages, location)

    df = pd.DataFrame(listings)
    df = preprocess(df)
    
    return df


def write(df):
    df.to_csv(f'C:/Users/USER/Desktop/Blossom_Academy/stream/meqasa_data.csv', index=False)
    print('Data written!')

    
if __name__ == '__main__':
    data = scrape_meqasa()
    write(data)