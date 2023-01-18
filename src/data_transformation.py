import sys

import numpy as np

from src.logger.logger import logger
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Transformation").getOrCreate()
import pandas as pd
import numpy

path =r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\Order_Data.csv"
path1=r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\exchange_rate.csv"
path2=r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\Cost_Data.csv"
path3=r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\IDs.csv"

spark = SparkSession.builder.appName("Transform_Data").getOrCreate()

#for googlesheet api
from googleapiclient.discovery import build
from google.oauth2 import service_account
SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
gsheet_id = '1Gt66NVoVjdZ7a4-OQ_DQfu6WJJCMQOfwVDb7Tr27gQE'
cred= None
cred = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE,scopes=SCOPES)
service = build('sheets', 'v4',credentials=cred)
sheet = service.spreadsheets()


def read_cleansed_data():
    try:
        logger.info(f"Reading cleansed_order_data File")
        order_data_df = spark.read.format("csv").option("Delimiter", ",").option("header", "true").load(path)
        logger.info("cleansed_order_data File Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading cleansed_order_data file {str(e)}")
        sys.exit(400)

    try:
        logger.info(f"Reading exchange_rate File")
        exchange_rate_df=spark.read.format("csv").option("Delimiter",",").option("header","true").load(path1)
        logger.info("exchange_rate File Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading exchange_rate file {str(e)}")
        sys.exit(400)


    try:
        logger.info(f"Reading Cost_data File")
        Cost_data_df=spark.read.format("csv").option("Delimiter",",").option("header","true").load(path2)
        logger.info("Cost_data File Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading Cost_data file {str(e)}")
        sys.exit(400)

    try:
        logger.info(f"Reading ID File")
        ID_df=spark.read.format("csv").option("Delimiter",",").option("header","true").load(path3)
        logger.info("ID File Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading ID file {str(e)}")
        sys.exit(400)

#creating temp view for order_data
    order_data_df.createOrReplaceTempView("order_data")

    exchange_rate_df.createOrReplaceTempView("exchange_rate")

    ID_df.createOrReplaceTempView("IDs")

    Cost_data_df.createOrReplaceTempView("Cost_Data")

read_cleansed_data()


def Transformation():
#Total Revenue Calculation
    try:
        logger.info(f"Performing Total Revenue Transformation")
        final = spark.sql("with cte (select *,"
                          "(case when o.discount_currency_new='LKR' then (o.discount*EUR_Value) "
                          "when o.discount_currency_new='EUR' then (o.discount*1) end) as discount_in_euro "
                          "from order_data o "
                          "join exchange_rate e ON o.order_date = E.DATE "
                          "join IDs i  on o.shop_id = i.id) "
                          "select localcurrency as Local_Currency, (sum(revenue_before_discount) - sum(discount_in_euro)) as Total_Revenue_Euro "
                          "from cte  "
                          "group by localcurrency")
        final.show()
        logger.info("Total Revenue Transformation Successfully")

    except Exception as e:
        logger.exception(f"Error in Total Revenue Transformation {str(e)}")
        sys.exit(400)


    Cost_Data_Final = final.toPandas()
    Cost_Data_Final.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\Result_Set\Total_Revenue.csv',index=False)
    Total_Revenue_in_Euro=pd.read_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\Result_Set\Total_Revenue.csv')
    Total_Revenue_in_Euro.replace(np.nan, '', inplace=True)
    request = service.spreadsheets().values().update(
            spreadsheetId=gsheet_id,
            valueInputOption='RAW',
            range='cc!A1',
            body=dict(
                majorDimension='ROWS',
                values=Total_Revenue_in_Euro.T.reset_index().T.values.tolist())
        ).execute()


#Revenue Share Calculation
    try:
        logger.info(f"Performing Total Revenue Transformation")
        final = spark.sql("with cte as"
                          "(select *,(case when o.discount_currency_new='INR' then (o.discount*EUR_Value) "
                          "when o.discount_currency_new='EUR' then (o.discount*1) end) as discount_in_euro "
                          "from order_data o join EXCHANGE_RATE e ON o.order_date = E.DATE) "
                          "select product_category, (sum(revenue_before_discount) - sum(discount_in_euro)) as total_revenue "
                          "from cte  "
                          "group by product_category")

        final.createOrReplaceTempView("final")

        result = spark.sql("WITH revenue_sum AS "
                        "(SELECT SUM(total_revenue) as revenue_sum FROM final) "
                        "SELECT product_category, SUM(total_revenue) as category_revenue, "
                        "(SUM(total_revenue) / (SELECT revenue_sum FROM revenue_sum)) * 100 as revenue_share FROM final GROUP BY product_category order by revenue_share desc")
        result.show()

        logger.info("Total Revenue Transformation Successfully")

    except Exception as e:
        logger.exception(f"Error in reading data {str(e)}")
        sys.exit(400)


    Cost_Data_Final = result.toPandas()
    Cost_Data_Final.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\Result_Set\Revenue_Share.csv',mode ='w',index=False)
    Revenue_share_category_wise=pd.read_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\Result_Set\Revenue_Share.csv')
    Revenue_share_category_wise.replace(np.nan, '', inplace=True)
    request = service.spreadsheets().values().update(
            spreadsheetId=gsheet_id,
            valueInputOption='RAW',
            range='bb!A1',
            body=dict(
                majorDimension='ROWS',
                values=Revenue_share_category_wise.T.reset_index().T.values.tolist())
        ).execute()

#Cost-revenue Ratio Calculation
    try:
        logger.info(f"Performing Total Revenue Transformation")
        df1 = spark.sql("select c.date,c.shop_id,(c.advertising_costs_local_currency * e.EUR_Value) as ad_cost_in_euro from Cost_Data c join exchange_rate e on c.date = e.date")
        df1.createOrReplaceTempView("df2")
        df3 = spark.sql("select sum(ad_cost_in_euro) as add_cost from df2")
        df3.createOrReplaceTempView("df3")

        final = spark.sql("with cte as"
                          "(select *,(case when o.discount_currency_new='INR' then (o.discount*EUR_Value) "
                          "when o.discount_currency_new='EUR' then (o.discount*1) end) as discount_in_euro "
                          "from order_data o join EXCHANGE_RATE e ON o.order_date = E.DATE) "
                          "select product_category,sum(discount_in_euro) as discount_sum, (sum(revenue_before_discount) - sum(discount_in_euro)) as total_revenue "
                          "from cte  "
                          "group by product_category")

        final.createOrReplaceTempView("final")
        crr = spark.sql("select sum(discount_sum) * -1 as discount_sum, sum(total_revenue) as total_revenue from final ")
        crr.createOrReplaceTempView("crr")
        crr1 = spark.sql("select * from crr, df3")

        crr1.createOrReplaceTempView("Final_Crr")

        Final_Result = spark.sql("select ((discount_sum)+(add_cost))/(total_revenue) as crr from Final_Crr")
        Final_Result.show()

        logger.info("Total Revenue Transformation Successfully")

    except Exception as e:
        logger.exception(f"Error in reading data {str(e)}")
        sys.exit(400)


    Cost_Data_Final = Final_Result.toPandas()
    Cost_Data_Final.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\Result_Set\CRR.csv',mode ='w',index=False)
    CRR=pd.read_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\Result_Set\CRR.csv')
    CRR.replace(np.nan, '', inplace=True)
    request = service.spreadsheets().values().update(
            spreadsheetId=gsheet_id,
            valueInputOption='RAW',
            range='aa!A1',
            body=dict(
                majorDimension='ROWS',
                values=CRR.T.reset_index().T.values.tolist())
        ).execute()
Transformation()