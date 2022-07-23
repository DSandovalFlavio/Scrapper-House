# Scrapper for the website https://www.vivanuncios.com.mx/s-venta-inmuebles/page-2/v1c1097p2

# %%
# Importing the libraries
# libraries for web scraping
from calendar import month
import requests
from bs4 import BeautifulSoup
import time
# libraries for data processing
import pandas as pd
import numpy as np
import re
import datetime
# libraries for saving data
from sqlalchemy import create_engine

# %%
# Create a dataframe with the data of the ads
def get_data_ad(link_root, ad):
    # get the title of the ad and link to the ad - <a class="href-link tile-title-text"
    ad_info = ad.find('a', class_='href-link tile-title-text')
    ad_link = ad_info['href']
    ad_type_link = ad_link.split('-')[0].replace('/', '')
    ad_title = ad_info.text
    # get and clean price 
    price = ad.find('span', class_='ad-price')
    ad_price = 0 if price is None else int(re.sub('[^0-9]', '', price.text))
    # get atributes of the ad - <div class="additional-attributes-container">
    ad_attributes = ad.find('div', class_='additional-attributes-container')
    room = ad_attributes.find('div', class_='chiplets-inline-block re-bedroom')
    ad_room = 0 if room is None else int(re.sub('[^0-9]', '', room.text))
    bathroom = ad_attributes.find('div', class_='chiplets-inline-block re-bathroom')
    ad_bathroom = 0 if bathroom is None else int(re.sub('[^0-9]', '', bathroom.text))
    car_parking = ad_attributes.find('div', class_='chiplets-inline-block car-parking')
    ad_car_parking = 0 if car_parking is None else int(re.sub('[^0-9]', '', car_parking.text))
    surface_area = ad_attributes.find('div', class_='chiplets-inline-block surface-area')
    ad_suftace_area = 0 if surface_area is None else int(re.sub('[^0-9]', '', surface_area.text))
    # get count photos of the ad - <apan class="tile-photo-count">
    photo = ad.find('span', class_='tile-photo-count')
    ad_count_photo = 0 if photo is None else int(re.sub('[^0-9]', '', photo.text))
    # get atributes promotion of the ad - <div class="tile-promotion-container">
    ad_all_promotion = ad.find('div', class_='tile-promotion-container')
    ad_urgent_promotion = 'Urgente' if ad_all_promotion.find('span', class_='tile-promotion urgent one-liner') is not None else '-'
    ad_top_promotion = 'Destacado' if ad_all_promotion.find('span', class_='tile-promotion top one-liner') is not None else '-'
    type_new = ad_all_promotion.find('span', class_='tile-promotion presale one-liner')
    ad_type_new = type_new.text if type_new is not None else '-'
    # get link the ad - <div class="tile-desc one-liner">
    ad_deep_link = ad.find('div', class_="tile-desc one-liner")
    # get the link to the ad - <a class="href-link tile-title-text"
    ad_link = ad_deep_link.find('a', class_='href-link tile-title-text')['href']
    # request the ad link
    ad_page = requests.get(link_root + ad_link)
    soup_ad_page = BeautifulSoup(ad_page.content, 'html.parser')
    #print(link_root + ad_link)
    # fix lost data
    # get <div class="revip-general-details"
    table_data = soup_ad_page.find('div', class_='revip-general-details')
    if table_data is None:
        extra_data = '[]'
    else:
        extra_data = str([item.text for item in table_data.find_all('div', class_='category-inner-container')])
    # get user_name - <div class="profile-username">
    name = soup_ad_page.find('div', class_='profile-username')
    user_name = '' if name is None else name.text
    # get date of the ad - <div class="last-post">
    date_inf_ad = soup_ad_page.find('div', class_='last-post')
    if date_inf_ad is not None:
        date_inf_ad = date_inf_ad.text
        date_inf_ad = date_inf_ad.replace('un', '1')
        ad_date = float(re.sub('[^0-9]', '', date_inf_ad.split('|')[0]))
        ad_visits = float(re.sub('[^0-9]', '', date_inf_ad.split('|')[1]))
    else:
        ad_date = 0
        ad_visits = 0
    # date published
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    day = 1
    date_today = datetime.datetime(year, month, day)
    subtract_month = ad_date * 30
    # substract the month of the date_today
    date_published = (date_today - datetime.timedelta(days=subtract_month)).date()
    # convert to json
    soup_ad_page_json = soup_ad_page.find('script', type='application/ld+json')
    if soup_ad_page_json is not None:
        ad_json = soup_ad_page_json.text
        # obtain text next geo with regex
        geo_text = re.search(r'latitude":(.*?),.*?longitude":(.*?)}', ad_json)
    else:
        geo_text = None
    if geo_text is not None:
        ad_latitude = float(geo_text.group(1))
        ad_longitude = float(geo_text.group(2))
    else:
        ad_latitude = 0
        ad_longitude = 0
    # get address - <div class="location-name">
    address = soup_ad_page.find('div', class_='location-name')
    ad_address = '' if address is None else address.text
    # get description - <div class="description-container">
    description = soup_ad_page.find('div', class_="description-content")
    ad_description = '-' if description is None else re.sub('<.*?>', '', str(description.text))
    # crate dataframe with the information
    df_ad = pd.DataFrame(data={ 'title': [ad_title],
                            'link': [ad_link],
                            'type_link': [ad_type_link],
                            'user_name': [user_name],
                            'price': [ad_price],
                            'last_month_publish': [ad_date],
                            'date_publish': [date_published],
                            'visits': [ad_visits],
                            'room': [ad_room],
                            'bathroom': [ad_bathroom],
                            'car_parking': [ad_car_parking],
                            'suftace_area': [ad_suftace_area],
                            'count_photo': [ad_count_photo],
                            'urgent_promotion': [ad_urgent_promotion],
                            'top_promotion': [ad_top_promotion],
                            'presale_promotion': [ad_type_new],
                            'address': [ad_address],
                            'ad_latitude': [ad_latitude],
                            'ad_longitude': [ad_longitude],
                            'extra_data': [extra_data],
                            'description': [ad_description]})
    return df_ad

# %%
# get the information of the ads
full_data = pd.DataFrame()
for num_page in range(1000):
    num_page = str(num_page)
    print(f'******************page {num_page}******************')
    # Declare the URL to be scraped
    link_root = 'https://www.vivanuncios.com.mx/'
    link_main = f'https://www.vivanuncios.com.mx/s-venta-inmuebles/page-{num_page}/v1c1097p{num_page}'
    print(link_main)
    # Scraping the data with requests and BeautifulSoup
    page = requests.get(link_main)
    soup = BeautifulSoup(page.content, 'html.parser')
    # extract class="tileV2 REAdTileV2 promoted listView"
    list_data_promoted = soup.find_all('div', class_='tileV2 REAdTileV2 promoted listView')
    # extract class="tileV2 REAdTileV2 regular listView"
    list_data_regular = soup.find_all('div', class_='tileV2 REAdTileV2 regular listView')
    # generate dataframe
    for ad in list_data_promoted:
        try:
            df_ad = get_data_ad(link_root, ad)
            df_ad = df_ad.assign(ad_type = 'promoted')
            df_ad = df_ad.assign(ad_page = num_page)
            full_data = pd.concat([full_data, df_ad], ignore_index=True)
        except:
            print('error')
            pass
    for ad in list_data_regular:
        try:
            df_ad = get_data_ad(link_root, ad)
            df_ad = df_ad.assign(ad_type = 'regular')
            df_ad = df_ad.assign(ad_page = num_page)
            full_data = pd.concat([full_data, df_ad], ignore_index=True)
        except:
            print('error')
            pass
    # wait 5 seconds for the next request page
    time.sleep(5)
    
# %%
# add column with the date of the scraping
full_data = full_data.assign(date_scraping = datetime.datetime.now().date())
# add column with the source of the data
full_data = full_data.assign(source = 'vivanuncios')
# save the data
full_data.to_csv('data_ad.csv', index=False)

# %%
