import sys
import pandas as pd
import requests
from src.logger.logger import logger

# base currency or reference currency
base="LKR"
# required currency for plot
out_curr="EUR"
# exchange data from a date
start_date="2022-02-15"
# exchange data till a date
end_date="2022-03-15"

# api url for request
def exchange_rate():
    try:
        logger.info(f"Connecting to exchange_api")
        url = 'https://api.exchangerate.host/timeseries?base={0}&start_date={1}&end_date={2}&symbols={3}'.format(base,start_date,end_date,out_curr)
        response = requests.get(url)
        data = response.json()
        data.keys()
        df = pd.DataFrame(data['rates'])
        df1 = df.transpose()
        df1.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\BBGAssesment\src\input_data\exchange_rate.csv')
        logger.info("exchange_api Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading data {str(e)}")
        sys.exit(400)

exchange_rate()

