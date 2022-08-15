""" INSTALL THE FOLLOWING LIBRARIES IF YOU THINK YOU DON'T HAVE THEM INSTALLED ALREADY"""
# !pip install urllib.request
# !pip install numpy 
# !pip install pandas 
# !pip install sqlalchemy 
# !pip install argparse
# !pip install requests
# !pip install mysql-connector-python

# Import all the necessary libraries
import pandas as pd
import numpy as np
import sqlalchemy
import argparse
import requests
import gzip
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import argparse

""" Create arguments parsing to allow the users to specify the category of the data they want """
parser = argparse.ArgumentParser()
parser.add_argument('--category', help='python ./load_data.py --category Category_Name')
args = parser.parse_args()
print("You parsed the following arguments: ", args) # Debugging line, to be dropped in the final script

""" Connect to the database. REMEMBER YOU HAVE TO BE CONNECTED TO EMORY VPN """
config = {
  'user': 'NET ID',
  'password': 'yourPassword',
  'host': 'msba-bootcamp-prod.cneftpdd0l3q.us-east-1.rds.amazonaws.com',
  'database': 'MSBA_Team13',
  'raise_on_warnings': True
}

# Connect to the database with
try:
    cnx = mysql.connector.connect(**config)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cnx.close()


"""get the gzipped file name from the website link."""
# Get the category from the parsed argument
category = str(args).split(sep='\'')[1]
# Get the web link to the dataset of the specified category
def getCategoryURL(category: str) ->str:
    url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt"
    filename = "index.txt"
    request = requests.get(url, allow_redirects=True)
    open(filename, 'wb').write(request.content)
    with open(filename,'r') as f:
        lines = f.readlines()
    for line in lines:
        try:
            line.upper().index(category.upper())
        except ValueError:
            # print("Not found!")
            continue
        else:
            print(category + " Category is Found!")
            print("The full URL is: "+line)
            return line
            break

# Download the data file from the web

url = getCategoryURL("gift_card")

#Get file name from the url
def getDataFilename_fromLink(weblink: str) -> str:
    filename = weblink.split('/')[-1]
    # drop the \n that comes in through readlines method
    filename = filename.split('\n')[0]
    return filename

# Download the data file and save it on local machine
filename = getDataFilename_fromLink(url)
print(filename)
request = requests.get(url.strip('\n'), allow_redirects=True)
open(filename, 'wb').write(request.content)
print("The gzipped file saved under the name: ", filename)

# Unzip the gz file and write the contents into pandas dataframe
with gzip.open(filename, 'rb') as f:
    dataFrame1 = pd.read_csv(f, sep = '\t')

# Create a table in the database
DB_NAME = 'MSBA_Team13'
# Creeate an empty dictionary to store table names and their respective SQL design codes 
TABLES = {}
# Design the tables using the SQL scripts
TABLES['reviews_raw'] = (
    "CREATE TABLE `reviews_raw` ("
    "  `marketplace` varchar(32),"
    "  `customer_id` int(16) NOT NULL,"
    "  `review_id` varchar(32) NOT NULL,"
    "  `product_id` varchar(16) NOT NULL,"
    "  `product_parent` int(16) NOT NULL,"
    "  `product_title` varchar(256) NOT NULL,"
    "  `product_category` varchar(32) NOT NULL,"
    "  `star_rating` int(1) NOT NULL,"
    "  `helpful_votes` int(10) NOT NULL,"
    "  `total_votes` int(10) NOT NULL,"
    "  `vine` enum('Y','N') NOT NULL,"
    "  `verified_purchase` enum('Y','N') NOT NULL,"
    "  `review_headline` varchar(128) NOT NULL,"
    "  `review_body` varchar(128) NOT NULL,"
    "  `review_date` date NOT NULL,"
    "  PRIMARY KEY (`review_id`)"
    ") ENGINE=InnoDB")

# Create table designed above
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()


# Insert the Data in the table using the SQL connector
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

tomorrow = datetime.now().date() + timedelta(days=1)

add_review = """INSERT INTO reviews_raw(
                                    marketplace, 
                                    customer_id,
                                    review_id,
                                    product_id,
                                    product_parent,
                                    product_title,
                                    product_category,
                                    star_rating,
                                    helpful_votes,
                                    total_votes,
                                    vine,
                                    verified_purchase,
                                    review_headline,
                                    review_body,
                                    review_date)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

for i in range(dataFrame1.shape[0]):
    data_review = tuple(str(element) for element in dataFrame1.iloc[i])
    # Insert new review
    cursor.execute(add_review, data_review)


# Make sure data is committed to the database
cnx.commit()

cursor.close()
cnx.close()