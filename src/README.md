Objective:
The objective is to perform the ETL(Extract-Transform-Load) using
process by reading the given dataset which is one of the source and Connect to any open-source currency exchange API for the 
exchange rates which is another dataset source, 
cleaning the dataset in the desired form and making the necessary transformation to calculate the KPI requested and
export the resultant data set to the google sheet.


Installation instructions:
Kindly find all the installed packages in the requirement.txt file.

Methodology:

Stage1:
Data Extraction
Moved all the provided input data file with the same give format to the Input_data folder.
Connected to ExchangeRate-API to get the exchange rate of local currency(LKR-SRILANKAN RUPEE) and stored it in csv file format 
in Input_data folder.

Stage2:
Data Cleansing 
Validated the given data for corrupted values and performed data cleansing activity on the given data set and stored it in
Pre_Processed folder in csv file format.

Stage3:
Data transformation:
Using pyspark function's and pyspark sql function's performed transformation on the given data set to achieve the
necessary KPI.

Stage4:
Load and save:
After transforming the final data is loaded into result_set in csv file format's.
From there data is exported to google sheet via api.


Note : app.py id our main function 