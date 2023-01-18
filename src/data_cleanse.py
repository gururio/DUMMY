import sys
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,lit,to_date,split
from pyspark.sql.functions import col
from pyspark.sql.types import *
from src.logger.logger import logger

spark = SparkSession.builder.appName("Read_data").getOrCreate()

filePath=(r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\input_data\Order_Data.xlsx")
filePath1=(r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\input_data\Cost_Data.xlsx")
filePath2 =r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\input_data\IDs.csv"
filePath3 =r"C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\input_data\exchange_rate.csv"

def data_clean():
# Cleansing order_data
    try:
        logger.info(f"Reading order_data Input File")
        Order_Data_DF = pd.read_excel(io = filePath, engine='openpyxl', sheet_name = 'order_data')
        logger.info("order_data File Read Successfully")


    except Exception as e:
        logger.exception(f"Error in reading order_data data {str(e)}")
        sys.exit(400)

    Order_Data_DF_schema = StructType([StructField("order_date", DateType (), True)\
                           ,StructField("year",IntegerType(), True)\
                           ,StructField("month", IntegerType(), True)\
                           ,StructField("day", IntegerType(), True)\
                           ,StructField("shop_id", LongType(), True)\
                           ,StructField("customer_id", StringType(), True)\
                           ,StructField("product_category", StringType(), True)\
                           ,StructField("revenue_before_discount (in euro)", StringType(), True)\
                           ,StructField("discount", StringType(), True)\
                           ,StructField("discount_currency", StringType(), True)])

    Order_Data_DF1 = spark.createDataFrame(Order_Data_DF, schema=Order_Data_DF_schema)

    try:
        logger.info(f"Cleansing Order_data")
        Order_Data_DF2 = Order_Data_DF1.withColumn("discount_currency", F.regexp_replace(col("discount_currency"), "[^a-zA-Z0-9]", ""))\
                       .withColumnRenamed('revenue_before_discount (in euro)','revenue_before_discount')\
                       .withColumn("discount_currency_new", when(col("discount_currency").rlike("^local|LOCAL"),lit("LKR")).otherwise(lit("EUR")))\
                       .withColumn('order_date',to_date("order_date","yyyy/MM/dd"))\
                       .na.fill(value='Not_Defined',subset='product_category')\
                       .drop('discount_currency')
        logger.info("Cleansing Order_data successful")

    except Exception as e:
        logger.exception(f"Error in Cleansing Order_data {str(e)}")
        sys.exit(400)

    Order_Data_Final = Order_Data_DF2.toPandas()

    Order_Data_Final.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\Order_Data.csv',mode ='w',index=False)

#Cleansing AdCosts

    try:
        logger.info(f"Reading Ad_Costs File")
        Cost_Data_DF = pd.read_excel(io = filePath1, engine='openpyxl', sheet_name = 'AdCosts')
        logger.info("Ad_Costs File Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading Ad_Costs data {str(e)}")
        sys.exit(400)

    Cost_Data_DF_schema = StructType([StructField("date", DateType (), True)\
                           ,StructField("Shop_id",LongType(), True)\
                           ,StructField("advertising_costs (local currency)", StringType(), True)])

    Cost_Data_DF1 = spark.createDataFrame(Cost_Data_DF, schema=Cost_Data_DF_schema)

    try:
        logger.info(f"Cleansing Ad_Costs")
        Cost_Data_DF2 = Cost_Data_DF1.withColumnRenamed('advertising_costs (local currency)','advertising_costs_local_currency')
        logger.info("Cleansing Ad_Costs successful")

    except Exception as e:
        logger.exception(f"Error in Cleansing Ad_Costs {str(e)}")
        sys.exit(400)

    Cost_Data_Final = Cost_Data_DF2.toPandas()
    Cost_Data_Final.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\Cost_Data.csv',mode ='w',index=False)

# Cleansing ID's

    try:
        logger.info(f"Reading Input ID File")
        IDs_DF = spark.read.format("csv").option("Delimiter",";").option("header","true").load(filePath2)
        logger.info("ID File Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading ID data {str(e)}")
        sys.exit(400)

    try:
        logger.info(f"Cleansing ID data")
        IDs_DF1 = IDs_DF.withColumnRenamed('Shop Name ','ShopName').withColumnRenamed("Local Currency Abbreviation","LocalCurrency")
        IDs_DF2 = IDs_DF1.withColumn("Shop_Name", split(col("ShopName"), " ").getItem(0)).withColumn("LocalCurrency", split(col("ShopName"), " ").getItem(1)).drop(IDs_DF1.ShopName)
        logger.info("Cleansing Ad_Costs successful")

    except Exception as e:
        logger.exception(f"Error in Cleansing ID data {str(e)}")
        sys.exit(400)

    IDs_Final = IDs_DF2.toPandas()

    IDs_Final.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\IDs.csv',mode ='w',index=False)


#Cleansing exchange_rate

    try:
        logger.info(f"Reading Input exchange_rate")
        Exc_Rate_DF = spark.read.format("csv").option("Delimiter",",").option("header","true").load(filePath3)
        logger.info("exchange_rate File Read Successfully")

    except Exception as e:
        logger.exception(f"Error in reading exchange_rate data {str(e)}")
        sys.exit(400)


    try:
        logger.info(f"Cleansing exchange_rate data")
        Exc_Rate_DF1 = Exc_Rate_DF.withColumnRenamed('_c0','Date').withColumnRenamed("EUR","EUR_Value")
        logger.info("Cleansing exchange_rate successful")

    except Exception as e:
        logger.exception(f"Error in Cleansing exchange_rate data {str(e)}")
        sys.exit(400)

    Exc_Rate_Final = Exc_Rate_DF1.toPandas()
    Exc_Rate_Final.to_csv(r'C:\Users\Nivetha Vijayakumar\PycharmProjects\DUMMY\src\pre_processed\exchange_rate.csv',mode ='w',index=False)

data_clean()