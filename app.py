import sys
from src.logger.logger import logger
from src.exchange_rate import exchange_rate
from src.data_cleanse import data_clean
from src.data_transformation import read_cleansed_data,Transformation

if __name__ == "__main__":
    try:
        logger.info("Initiated ETL Process")
        #process initiated
        fetching_exchange_rate = exchange_rate
        #Fetching exchange rate from money exchange api
        data_cleansing = data_clean()
        #data cleansing
        read_cleansed = read_cleansed_data()
        #reading cleansed data
        transform_cleansed = Transformation()
        #Transforming cleansed data
    except Exception as e:
        logger.exception(f"An error occurred while executing main function {str(e)}")
        sys.exit(400)

