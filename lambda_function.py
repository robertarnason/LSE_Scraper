from bs4 import BeautifulSoup
from requests import get
import pandas as pd
import datetime
import boto3
import logging
from io import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_URLs():
    FTSE_100_urls = ['https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=UKX']

    FTSE_100_response = get(FTSE_100_urls[0])

    FTSE_100_html_soup = BeautifulSoup(FTSE_100_response.content, 'html.parser')

    page_soup = FTSE_100_html_soup.find('div', class_ = 'paging')

    page_list = page_soup.find_all('a', href = True)

    for page in page_list:
        FTSE_100_urls.append('https://www.londonstockexchange.com' + page['href'])
        
    company_codes = list()
    company_names = list()
    urls = list()

    for url in FTSE_100_urls[:-1]:
        FTSE_100_response = get(url)
        FTSE_100_html_soup = BeautifulSoup(FTSE_100_response.content, 'html.parser')

        ftse100table = FTSE_100_html_soup.find('table')

        rows = ftse100table.find_all(class_ = {'odd','even'})

        for row in rows:
            cells = row.find_all("td")
            comp_code = cells[0].get_text()
            comp_name = cells[1].get_text().replace('\n','')
            comp_url = row.find('a', href = True)['href']

            company_codes.append(comp_code)
            company_names.append(comp_name)
            urls.append(comp_url)

    ftse100_column_names = ['Company Code', 'Company Name', 'URL']
    temp_list = list(zip(company_codes, company_names, urls))

    ftse100_url_df = pd.DataFrame(temp_list, columns = ftse100_column_names)
        
    return ftse100_url_df
    

def load_or_create_dataframe(file_name, s3_client, bucket):#, logger):
    #date_today = datetime.datetime.now()
    #this_months_title = 'FTSE_Data_' +'{:02d}'.format(date_today.month) + '_' + str(date_today.year)

    try:
        response = s3_client.get_object(Bucket = bucket, Key = file_name)

        csv_file = response["Body"]
    
        # Load csv as a Pandas Dataframe
        this_months_df = pd.read_csv(csv_file, index_col=0, low_memory=False)
        #this_months_df = pd.read_csv(file_name, index_col = 0)
        logger.info('Data exists for this month, loading previous Pandas dataframe.')
    except:
        column_names = ['Scrape Time', 'Company Code', 'Company Name', 'Main Table']
        logger.info('No data for this month, creating new Pandas dataframe.')

        this_months_df = pd.DataFrame(columns = column_names)
        
    return this_months_df


def save_dataframe(df_to_save, file_name, s3_resource, bucket):
    #date_today = datetime.datetime.now()
    #this_months_title = 'FTSE_Data_' +'{:02d}'.format(date_today.month) + '_' + str(date_today.year)

    csv_buffer = StringIO()
    
    # Write dataframe to buffer
    df_to_save.to_csv(csv_buffer)
    s3_resource.Object(bucket, f'{file_name}').put(Body=csv_buffer.getvalue())
    #df_to_save.to_csv(file_name)
    
    
def scrape_stockdata(URL_df):
    column_names = ['Scrape Time', 'Company Code', 'Company Name', 'Main Table']
    
    current_df = pd.DataFrame(columns = column_names)
    for i, row in URL_df.iterrows():
        response = get('https://www.londonstockexchange.com' + row['URL'])

        html_soup = BeautifulSoup(response.content, 'html.parser')

        table = html_soup.find('table')

        temp_list = [[datetime.datetime.now(), row['Company Code'], row['Company Name'], str(table).replace('\n', '').replace('\r', '')]]
        current_df = current_df.append(pd.DataFrame(temp_list, columns = column_names), ignore_index=True)

    return current_df


def lambda_handler(event, context):
    
    date_today = datetime.datetime.now()
    month_file_name = 'FTSE_Data_' +'{:02d}'.format(date_today.month) + '_' + str(date_today.year) + '.csv'
    
    access_key_id = 'AKIARQ74HTM7YYCX4GX7'
    access_secret_key = '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'
    bucket = 'lsescraper'
    
    s3_client = boto3.client('s3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=access_secret_key
        )
        
    s3_resource = boto3.resource('s3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=access_secret_key
        )
    
    try:
        ftse100_url_df = get_URLs()
        logger.info('Extracted URLs straight from LSE website')
        #save_dataframe(ftse100_url_df, 'FTSE_100_URLs.csv', s3_resource, bucket)
    except:
        response = s3_client.get_object(Bucket = bucket, Key = 'FTSE_100_URLs.csv')
    
        csv_file = response["Body"]
    
        # Load csv as a Pandas Dataframe
        ftse100_url_df = pd.read_csv(csv_file, index_col=0, low_memory=False)
        logger.info('Failed to extract URLs straight from LSE website!')
    
    monthly_df = load_or_create_dataframe(month_file_name, s3_client, bucket)#, logger)
    logger.info('Starting to scrape stock data.')
    try:
        current_df = scrape_stockdata(ftse100_url_df)
        monthly_df = monthly_df.append(current_df, ignore_index=True)
        
        save_dataframe(monthly_df, month_file_name, s3_resource, bucket)
        
        logger.info('Stock scraping successful!')
    except:
        logger.info('Stock scraping unsuccessful!')