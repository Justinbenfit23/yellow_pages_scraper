from pandas.core.arrays.sparse import dtype
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests
from requests import exceptions as ex
import pandas as pd
import numpy as np
import math
from pandas import *
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import re
from bs4 import BeautifulSoup
from time import sleep
import time
import psycopg2
from datetime import date

###############################################
# Testing
################################################
# Database
#This section would need to change for prod
DB_HOST = "localhost"
DB_NAME = "yellow_pages_scraper"
DB_USER = "justinbenfit"
DB_PASS = "postgres"
DB_URL = "postgresql://justinbenfit:postgres@localHost:5432/yellow_pages_scraper"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
cur = conn.cursor()



# cur.execute("""CREATE TABLE yellow_pages (ID SERIAL Primary Key,
# company Text NUll,
# phone Text NUll,
# website Text NUll,
# address Text NUll,
# street Text NUll,
# city Text NUll,
# postal_code Text NOT NULL,
# state Text NUll,
# first_name Text NUll,
# last_name Text NUll,
# weekly_scrape Text NUll,
# strikezone Text NUll,
# lead_source Text NUll,
# country Text NUll,
# acquisition_source Text NUll,
# UNIQUE (company, website)
# );
#     """)

# cur.execute("""ALTER TABLE yellow_pages ALTER COLUMN postal_code TYPE TEXT;""")

# cur.execute("""DROP TABLE yellow_pages""")


# conn.commit()
# cur.close()
# conn.close()

###############################################

##############################################
# Parameters
territory_name = 'Michigan121' #"New York206"   
strikezones = [
      "Plumbing",
      "Pest Control",
      "Pools",
      "Interior Remodeling",
      "Lawn Care",
      "Landscaping",
      "Garage Door",
      "Solar",
      "OEM Auto Dealer",
      "HVAC",
      "Jewelry",
      "Furniture",
      "Roofing",
      "Blinds",
      "Restoration",
      "Used Car Dealers",
      "Electrical",
      "Spa Dealer",
      "Appliance Store",
      "Appliance Repair",
      "Pawn Shop",
      "Fence",
      "Window Treatment",
      "Home Security",
      "Auto Recyclers",
      "Paul Davis Restoration"
    ]
postal_code_count = 200 
max_page = 10
today = date.today()
table = "yellow_pages"
first_name = '.'
last_name = '.' 
lead_source = 'Moneyball'
country = 'United states'
acquisition_source = 'yellow_pages_scrape'
weekly_scrape = f"yellow_pages_scrape{today}"

# ##############################################
# Inception of the Territory Table that I'll use to specify what zip codes to scrape

def add_zero(postal_codes):
    new_list = []
    for p in postal_codes:
        if len(p) < 5:
            p = "0"+p
            new_list.append(p)
        else: 
            new_list.append(p)
    return new_list

territories_to_scrape = pd.read_csv("/Users/justinbenfit/Desktop/yellowpages_scrape/territories_postal_codes.csv")
territories_to_scrape = territories_to_scrape.loc[17:]
# territories_to_scrape.query(f'territory_name == "{territory_name}"', inplace=True)
# territories_to_scrape = territories_to_scrape.head(postal_code_count) #KEEP THESE TWO TURNED OFF UNLESS WANT TO SCRAPE SPECIFIC TERRITORIES
postal_codes = list(territories_to_scrape["postal_code"])
postal_codes = [str(x) for x in postal_codes]
postal_codes = add_zero(postal_codes)
# postal_codes = ['14626', '14625', '14624', '14623', '14622', '14621', '14620', '14619', '14618', '14617', '14616', '14615', '14614', '14613', '14612', '14611', '14610', '14609', '14608', '14607', '14606', '14605', '14604', '14603', '14602', '14601', '14592', '14591', '14590', '14589', '14588', '14586', '14585', '14580', '14572', '14571', '14569', '14568', '14564', '14563', '14561', '14560', '14559', '14558', '14557', '14556', '14555', '14551', '14550', '14549', '14548', '14547', '14546', '14545', '14544', '14543', '14542', '14541', '14539', '14538', '14537', '14536', '14534', '14533', '14532', '14530', '14529', '14527', '14526', '14525', '14522', '14521', '14520', '14519', '14518', '14517', '14516', '14515', '14514', '14513', '14512', '14511', '14510', '14508', '14507', '14506', '14505', '14504', '14502', '14489', '14488', '14487', '14486', '14485', '14482', '14481', '14480', '14479', '14478', '14477', '14476', '14475', '14472', '14471', '14470', '14469', '14468', '14467', '14466', '14464', '14463', '14462', '14461', '14456', '14454', '14453', '14452', '14450', '14449', '14445', '14443', '14441', '14437', '14435', '14433', '14432', '14430', '14429', '14428', '14427', '14425', '14424', '14423', '14422', '14420', '14418', '14416', '14415', '14414', '14413', '14411', '14410', '14305', '14304', '14303', '14302', '14301', '14280', '14276', '14273', '14272', '14270', '14269', '14267', '14265', '14264', '14263', '14261', '14260', '14241', '14240', '14233', '14231', '14228', '14227', '14226', '14225', '14224', '14223', '14222', '8870', '8834', '8803', '8557', '8556', '8099', '7829']
# postal_codes = ['48227', '48226', '48225', '48224', '48223', '48222', '48221', '48220', '48219', '48218', '48217', '48216', '48215', '48214', '48213', '48212', '48211', '48210', '48209', '48208', '48207', '48206', '48205', '48204', '48203', '48202', '48201', '48198', '48197', '48195', '48193', '48192', '48191', '48190', '48189', '48188', '48187', '48186', '48185', '48184', '48183', '48182', '48180', '48179', '48178', '48177', '48176', '48175', '48174', '48173', '48170', '48169', '48168']



df = pd.DataFrame()
chrome_driver_path = '/Users/justinbenfit/Desktop/Programming/chromedriver'
option = webdriver.ChromeOptions()
option.add_argument('headless')
driver = webdriver.Chrome(executable_path=chrome_driver_path, options=option)
url = 'https://www.yellowpages.com/search?'


def run(max_page, strikezones, postal_codes, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape):
    tic = time.perf_counter()
    driver.get(url)
    for s in strikezones:
        for postal_code in postal_codes:
            send_keys(max_page, s, postal_code, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape)
    toc = time.perf_counter()
    print(f"Took {toc - tic:0.4f} seconds or {(toc - tic)/60} minutes, for run to complete for {len(postal_codes)} postal_codes")
    

def send_keys(max_page, strikezone_key, zip_code_key, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape):
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "search_terms")))
    except TimeoutException as t:
        print(f"{t} timed out!")
    else:
        element = driver.find_element(By.NAME, "search_terms")
        element.clear()
        element.send_keys(strikezone_key) 
        element = driver.find_element(By.NAME, "geo_location_terms")
        element.clear()
        element.send_keys(zip_code_key)
        driver.find_element_by_xpath("//button[@value='Find']").click()
        current_url = driver.current_url
        get_data_run(max_page, current_url, strikezone_key, zip_code_key, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape)

            
def get_data_run(max_page, current_url, strikezone_key, zip_code_key, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape):
    company_names = []
    phone_numbers = []
    websites = []
    addresses = []
    states = []
    postal_codes = []
    streets = []
    cities = []
    page = 1
   
    company_names = paginate(current_url, page, max_page, company_names, get_company_name)
    phone_numbers = paginate(current_url, page, max_page, phone_numbers, get_phone_numbers)
    websites = paginate(current_url, page, max_page, websites, get_websites)
    addresses = paginate(current_url, page, max_page, addresses, get_addresses)
    states = paginate(current_url, page, max_page, states, get_states)
    postal_codes = paginate(current_url, page, max_page, postal_codes, get_postal_codes)
    streets = paginate(current_url, page, max_page, streets, get_streets)
    cities = paginate(current_url, page, max_page, cities, get_cities)

    company_names = pd.Series(company_names)
    phone_numbers = pd.Series(phone_numbers)
    websites = pd.Series(websites)
    addresses = pd.Series(addresses)
    states = pd.Series(states)
    postal_codes = pd.Series(postal_codes)
    streets = pd.Series(streets)
    cities = pd.Series(cities)

    print(company_names)

    assign_to_df(company_names, phone_numbers, websites, addresses, states, postal_codes, streets, cities, strikezone_key, zip_code_key, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape) 


def paginate(current_url, page, max_page, params, callback):
    while (page <= max_page):
        trycnt = 3
        while (trycnt > 0):
            try:
                new_url = current_url + f"&page={page}"
                params1 = callback(new_url)
                params.extend(params1)
                
                page +=1
                trycnt = 0
            except Exception as e:
                if trycnt <= 0: print("Failed to retrieve: " + url + "\n" + srt(e))  # done retrying
                else: trycnt -= 1  # retry
                time.sleep(0.5)  # wait 1/2 second then retry
            if page == max_page:
                    return params

    return params

def get_company_name(url):
    company_names = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            company_names.append(box[i].find("a", {"class": "business-name"}).text.strip())
        except Exception:
            company_names.append("null")
        else: 
            continue
    return company_names
def get_phone_numbers(url):
    phone_numbers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            phone_numbers.append(box[i].find("div", {"class": "phones phone primary"}).text.strip())
        except Exception:
            phone_numbers.append("null")
        else: 
            continue
    return phone_numbers
def get_websites(url):
    websites = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            websites.append(box[i].find('a', attrs={'href': re.compile("^http://")}).get('href'))
        except Exception:
            websites.append("null")
        else: 
            continue
    return websites
def get_addresses(url):
    addresses = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            addresses.append(box[i].find("div", {"class": "adr"}).text.strip())
        except Exception:
            addresses.append("null")
        else: 
            continue
    return addresses
def get_states(url):
    states = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            states.append(re.search(r'(?:,\s)([A-Z]{2})',box[i].find("div", {"class": "adr"}).text.strip()).group(1))
        except Exception:
            states.append("null")
        else: 
            continue
    return states
def get_postal_codes(url):
    postal_codes = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            postal_codes.append(re.search(r'([0-9]{5})',box[i].find("div", {"class": "adr"}).text.strip()).group(1))
        except Exception:
            postal_codes.append(00000)
        else: 
            continue
    return postal_codes
def get_streets(url):
    streets = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            streets.append(box[i].find("div", {"class": "street-address"}).text.strip())
        except Exception:
            streets.append("null")
        else: 
            continue
    return streets
def get_cities(url):
    cities = []
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    box = list(soup.findAll("div", {"class": "v-card"}))
    for i in range(len(box)):
        try:
            cities.append(re.search(r'^(.+?),',box[i].find("div", {"class": "locality"}).text.strip()).group(1))
        except Exception:
            cities.append("null")
        else: 
            continue
    return cities

def assign_to_df(company_names, phone_numbers, websites, addresses, states, postal_codes, streets, cities, strikezone_key, zip_code_key, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape):
    df['company'] = company_names
    df['company'] = df['company'].str.replace("'","`")
    df['phone'] = phone_numbers
    df['website'] = websites
    df['address'] = addresses
    df['state'] = states
    df['first_name'] = first_name
    df['last_name'] = last_name
    df['lead_source'] = lead_source
    df['country'] = country
    df['acquisition_source'] = acquisition_source
    df['weekly_scrape'] = weekly_scrape
    df['strikezone'] = strikezone_key
    df['street'] = streets
    df['city'] = cities
    df['postal_code'] = postal_codes
    execute_values(conn, cur, df, table) 
    # print(dff.head(30))


def execute_values(conn, cur, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    temp_table = "temp_yellow_pages"
    cols = ', '.join(list(df.columns))
    tuples = [t for t in tuples if not any(isinstance(n, float) and math.isnan(n) for n in t)]
    
    # SQL query to execute
    temp_to_perm_query = f"""INSERT INTO {table} ({cols})
                                SELECT {cols}
                                FROM {temp_table}
                                ON CONFLICT (company, website) DO NOTHING;"""
    try:
        cur.execute('''CREATE TEMPORARY TABLE temp_yellow_pages (ID SERIAL Primary Key,
        company Text NUll,
        phone Text NUll,
        website Text NUll,
        address Text NUll,
        street Text NUll,
        city Text NUll,
        postal_code REAL NUll,
        state Text NUll,
        first_name Text NUll,
        last_name Text NUll,
        weekly_scrape Text NUll,
        strikezone Text NUll,
        lead_source Text NUll,
        country Text NUll,
        acquisition_source Text NUll
        );''')
        for t in tuples:
            cur.execute(f"INSERT INTO {temp_table}({cols}) VALUES {t};")
            cur.execute(temp_to_perm_query)
            conn.commit()
        # see_results(cur, table)
        cur.execute("""DROP TABLE temp_yellow_pages""")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        # cur.close()
        return 1
    print(f"the dataframe for {tuples[0]} is inserted")
    # cur.close()

def see_results(cur, table):
    print("complete")
    cur.execute(f"SELECT * from {table}")
    results = cur.fetchall()
    print(results)


run(max_page, strikezones, postal_codes, conn, cur, table, first_name, last_name, lead_source, country, acquisition_source, weekly_scrape)

conn.close()
driver.quit()
