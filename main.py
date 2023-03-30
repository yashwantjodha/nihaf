import pandas as pd

from pymongo import MongoClient

import mysql.connector
from mysql.connector import Error

import sys
import requests


##########
# CONFIG #
##########
db_client = MongoClient()
db_name = "email_automation"
db = db_client[db_name]
tags_collection = db["TAGS"]

# schema_type
df = pd.read_csv("./data/gmo_linkedin.csv").fillna('')
if "list_name" in df.columns:
    print("** Company schema inferred")
    schema_type = "company"
else:
    print("** Person schema inferred")
    schema_type = "person"

print()

# company_name
company_name = input("Enter company name: ").lower()
if company_name == "":
    print("** Error: company name cannot be empty")
    sys.exit()
if company_name in db.list_collection_names():
    print(f"** {company_name} already exists")
collection = db[company_name]

print()

# tag
while True:
    tag = input("Enter TAG(should be unique): ").lower()
    if collection.count_documents({"TAG": tag}):
        print(
            f"** Warning: '{tag}' tag is already in use, it should be unique for each list.."
        )
        continue
    break

print()

try:
    email_db_connection = mysql.connector.connect(
        host="localhost", database="world", user="root", password="root"
    )
    email_cursor = email_db_connection.cursor()
    email_read_cursor = email_db_connection.cursor(buffered=True)
except Error as e:
    print("** Error while connecting to MySQL", e)
    sys.exit()
    

######################
# EMAIL VERIFICATION #
######################
# create correct schema for email database
create_table_query = (
    "CREATE TABLE IF NOT EXISTS emails (ID INT AUTO_INCREMENT PRIMARY KEY,"
    "Email varchar(100) NOT NULL, StatusCode varchar(50), Result varchar(100), Description varchar(100),"
    "SMTPServer varchar(100), SMTPServerReply varchar(100), Date Date, Log varchar(100))"
)
email_cursor.execute(create_table_query)

# add data to mysql email database
query = "INSERT INTO emails VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
search_query = "SELECT * FROM emails WHERE email = %s"
email_fields = [f for f in df.columns if "email" in f]
for idx, row in df.iterrows():
    for email_col in email_fields:
        if row[email_col]:
            # duplication check
            email_read_cursor.execute(search_query, (row[email_col],))
            if not email_read_cursor.fetchone():
                email_cursor.execute(
                    query, (None, row[email_col], None, None, None, None, None, None, None)
                )
            
email_db_connection.commit()
print("** All emails inserted to email database")

print()

# run the email verification on new data
# TODO: use pywinauto to automate the gui process
print("** VERIFY EMAILS NOW: Open 'Email verifier' -> data source -> start (verify new)")
confirmation = ""
while confirmation != "y":
    confirmation = input("press 'y' and hit enter when verification is complete: ")

# reverse lookup emails in the database
query = "SELECT * FROM emails WHERE email = %s"
data = []
for idx, row in df.iterrows():
    doc = {}
    for x in df.columns:
        if x not in email_fields:
            doc[x] = row[x]

    doc['emails'] = []
    
    for email_field in email_fields:
        if row[email_field]:
            email_obj = {}
            email = row[email_field]
            email_read_cursor.execute(query, (email,))
            record = email_read_cursor.fetchone()
            if not record:
                raise Error("Email not found...")
            
            email_obj = {
                'Email': record[1],
                'StatusCode': record[2],
                'Result': record[3],
                'Description': record[4],
                'SMTPServer': record[5],
                'SMTPServerReply': record[6],
                'Date': record[7],
                'Log': record[8],
            }
            doc['emails'].append(email_obj)

    doc['ENRICHED'] = False
    doc['SCHEMA_TYPE'] = schema_type
    doc['TAG'] = tag
    data.append(doc)

collection.insert_many(data)
print(f'** data inserted to the {db_name}/{company_name}')
# TODO: create TAGS collection with tags and other details