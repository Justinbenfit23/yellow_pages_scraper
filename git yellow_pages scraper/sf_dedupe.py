from doctest import master
import pandas as pd
from pandas.core.frame import DataFrame
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import psycopg2
import psycopg2.extras
import textdistance
from datetime import date
from salesforce import get_sf_report

#################################################
#Inputs
table = 'yellow_pages'
territory_name = 'Michigan121'
reportId = '00O5G000008GHmxUAG'

# Database
#This section would need to change for prod
DB_HOST = "localhost"
DB_NAME = "yellow_pages_scraper"
DB_USER = "justinbenfit"
DB_PASS = "postgres"
DB_URL = "postgresql://justinbenfit:postgres@localHost:5432/yellow_pages_scraper"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
cur = conn.cursor()



# conn.commit()
# cur.close()
# conn.close()

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
territories_to_scrape['postal_code'] = territories_to_scrape['postal_code'].astype(str)
territories_to_scrape.query(f'territory_name == "{territory_name}"', inplace=True) #comment this out if you want to pull the whole db
postal_codes = list(territories_to_scrape["postal_code"])
postal_codes = add_zero(postal_codes)

#################################################
master_leads = get_sf_report(reportId) #Run this and next line then uncomment third line after doing an upload then comment out
master_leads.to_csv("master_leads.csv") #comment this out after commenting out the above
# master_leads = pd.read_csv('/Users/justinbenfit/Desktop/yellowpages_scrape/report1642183754603.csv')




def run(territories_to_scrape, cur, master_leads, table, postal_codes):
    company_name = get_company_name(master_leads)
    website = get_website(master_leads)
    postal_code = get_five_postal(master_leads)
    phone_col = get_phone_column(master_leads)
    master_cleaned = clean_text(master_leads, company_name)
    master_cleaned = clean_phone(master_cleaned)
    master_cleaned = clean_postal_codes(master_cleaned)

    
    insertion_data = query_inserts(cur, table, postal_codes)
    company_name = get_company_name(insertion_data)
    website = get_website(insertion_data)
    postal_code = get_five_postal(insertion_data)
    phone_col = get_phone_column(insertion_data)
    insertion_cleaned = clean_text(insertion_data, company_name)
    insertion_cleaned = clean_phone(insertion_cleaned)
    insertion_cleaned = clean_postal_codes(insertion_cleaned)
    
    fuzzy_match(territories_to_scrape, insertion_cleaned, master_cleaned) 


def clean_text(df, col):
    punc = r'[^\w\s]+'
    df[col] = df[col].str.replace(punc, '', regex=True).str.upper().str.strip()
    return df

    
def query_inserts(cur, table, postal_codes):
    postal_codes = tuple(postal_codes)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # cur.execute(f'''select * from {table} t
        #                 where t.postal_code in {postal_codes};''')
        cur.execute(f'''select yp.*, sv.name sub_vertical, v.name vertical
                        from yellow_pages yp
                        join strikezones sz on sz.name = yp.strikezone
                        join sub_verticals sv on sv.id = sz.sub_vertical_id
                        join verticals v on v.id = sv.vertical_id;''')
        
        rows = cur.fetchall()
        df = pd.DataFrame(rows)
    return df 

def clean_postal_codes(df):
    postal_col = get_five_postal(df)
    df[postal_col] = df[postal_col].str.extract(r'([0-9]{5})')
    return df

def clean_phone(df):
    phone_col = get_phone_column(df)
    df[phone_col] = df[phone_col].str.replace(r"[+()\s\s-]+|ext.\s.+","")
    df[phone_col] = df[phone_col].str.extract(r"(^[0-9]{10})")
    return df

def get_five_postal(df):
    postal_col = [col for col in df if col.lower().startswith('post') or col.lower().startswith('zip')][0]
    return postal_col
def get_company_name(df):
    company_name = [col for col in df if col.lower().startswith('comp')][0]
    return company_name
def get_website(df):
    website = [col for col in df if col.lower().startswith('web')][0]
    return website
def get_phone_column(df):
    phone = [col for col in df if col.lower().startswith('phone')][0]
    return phone
def get_state_column(df):
    state = [col for col in df if col.lower().startswith('state')][0]
    return state

def create_states_dict():
    abr_states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "RI", "ON", "AB", "BC", "DC", "NB"]
    state_names = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming", "Rhode Island", "Ontario", "Alberta", "British Columbia", "Washington D.C.", "New Brunswick"]
    zip_iterator = zip(abr_states, state_names)
    a_dictionary = dict(zip_iterator)
    return a_dictionary

def fuzzy_match(territories_to_scrape, df1, df2):
    states_dict = create_states_dict()

    company_col = get_company_name(df1)
    website_col = get_website(df1)
    postal_col = get_five_postal(df1)
    phone_col = get_phone_column(df1)
    state_col = get_state_column(df1)
    df1_with_postal = df1[df1[postal_col].notna()]
    df1_without_postal = df1[df1[postal_col].isna()]
    df1_without_postal = df1_without_postal[~df1_without_postal[website_col].isin(['0'])]
    df1_without_postal['state'].replace(states_dict, inplace=True)
    # print(df1_without_postal)

    df2company_col = get_company_name(df2)
    df2website_col = get_website(df2)
    df2postal_col = get_five_postal(df2)
    df2postal_col = get_five_postal(df2)
    df2phone_col = get_phone_column(df2)
    df2state_col = get_state_column(df2)
    df2_with_postal = df2[df2[df2postal_col].notna()]
    df2_without_postal = df2[df2[df2postal_col].isna()]

    territories_to_scrape_postal_col = get_five_postal(territories_to_scrape)

    df_with_postal = df1_with_postal.merge(df2_with_postal, how='left', left_on=postal_col, right_on=df2postal_col)
    df_with_postal = df_with_postal.dropna(subset=[postal_col])
    df_with_postal = df_with_postal[df_with_postal[company_col].notna()]
    df_with_postal = df_with_postal[df_with_postal[df2company_col].notna()]
    df_with_postal = df_with_postal[df_with_postal[website_col].notna()]
    df_with_postal = df_with_postal[df_with_postal[df2website_col].notna()]
    df_with_postal = df_with_postal[df_with_postal[phone_col].notna()]
    df_with_postal = df_with_postal[df_with_postal[df2phone_col].notna()]
    df_with_postal["company_distance"] = df_with_postal.loc[:, [company_col, df2company_col]].apply(lambda x: textdistance.jaro_winkler(*x), axis=1)
    df_with_postal["website_distance"] = df_with_postal.loc[:, [website_col, df2website_col]].apply(lambda x: textdistance.jaro_winkler(*x), axis=1)
    df_with_postal['mean_dist'] = df_with_postal[["company_distance", "website_distance"]].mean(axis=1)
    df_with_postal_mean_sorted = df_with_postal.sort_values("mean_dist", ascending=False)
    df_with_postal_mean_sorted = df_with_postal_mean_sorted[df_with_postal_mean_sorted['mean_dist'] > .84]
    df_with_postal_mean_sorted.to_csv("mean_sorted_with.csv")
    df_dupes_to_remove = df_with_postal_mean_sorted.sort_values('mean_dist').drop_duplicates('company', keep='last')
    final_df = df1[~df1['company'].isin(df_dupes_to_remove['company'])]
    final_df = final_df.merge(territories_to_scrape, how='inner', left_on=postal_col, right_on=territories_to_scrape_postal_col)
    final_df['weekly_scrape'] = final_df['acquisition_source'] + str(date.today()) 
    final_df["state"].replace(states_dict, inplace=True)
    final_df.set_index('id', inplace=True)
    final_df.to_csv("yellow_pages_leads_with_postal.csv") ####Figure out why this isn't working better to eliminate ones I just uploaded....
    
     

    df_without_postal = df1_without_postal.merge(df2_without_postal, how='left', left_on=state_col, right_on=df2state_col)
    df_without_postal = df_without_postal[df_without_postal[company_col].notna()]
    df_without_postal = df_without_postal[df_without_postal[df2company_col].notna()]
    df_without_postal = df_without_postal[df_without_postal[website_col].notna()]
    df_without_postal = df_without_postal[df_without_postal[df2website_col].notna()]
    df_without_postal = df_without_postal[df_without_postal[phone_col].notna()]
    df_without_postal = df_without_postal[df_without_postal[df2phone_col].notna()]
    df_without_postal["company_distance"] = df_without_postal.loc[:, [company_col, df2company_col]].apply(lambda x: textdistance.jaro_winkler(*x), axis=1)
    df_without_postal["website_distance"] = df_without_postal.loc[:, [website_col, df2website_col]].apply(lambda x: textdistance.jaro_winkler(*x), axis=1)
    df_without_postal['mean_dist'] = df_without_postal[["company_distance", "website_distance"]].mean(axis=1)
    df_without_postal_mean_sorted = df_without_postal.sort_values("mean_dist", ascending=False)
    df_without_postal_mean_sorted.to_csv("mean_sorted.csv")
    df_without_postal_mean_sorted = df_without_postal_mean_sorted[df_without_postal_mean_sorted['mean_dist'] > .84]
    df_dupes_without_postal_to_remove = df_without_postal_mean_sorted.sort_values('mean_dist').drop_duplicates('company', keep='last')
    final_df_without_postal = df1[~df1['company'].isin(df_dupes_without_postal_to_remove['company'])]
    final_df_without_postal['podium_territory_id'] = "aD05G00000004KP"
    final_df_without_postal.set_index('id', inplace=True)
    final_df_without_postal.to_csv("yellow_pages_leads_without_postal.csv")



    


run(territories_to_scrape, cur, master_leads, table, postal_codes)

conn.commit()
cur.close()
conn.close()