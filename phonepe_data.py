import os
import json
import pandas as pd

path=".../pulse-master/data/aggregated/transaction/country/india/state/"
state_list=os.listdir(path)

col = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],
       'Transaction_count': [], 'Transaction_amount': []}

for i in state_list:
    p1=path+i+"/"
    year=os.listdir(p1)
    for j in year:
        p2=p1+j+"/"
        qtr=os.listdir(p2)
        for k in qtr:
            p3=p2+k
            Data=open(p3,'r')
            D=json.load(Data)
            try:
                for x in D['data']['transactionData']:
                    Name=x['name']
                    count=x['paymentInstruments'][0]['count']
                    amount = x['paymentInstruments'][0]['amount']
                    col['Transaction_type'].append(Name)
                    col['Transaction_count'].append(count)
                    col['Transaction_amount'].append(amount)
                    col['State'].append(i)
                    col['Year'].append(j)
                    col['Quarter'].append(int(k.strip('.json')))
            except:
                pass

agg_transaction=pd.DataFrame(col)
agg_transaction.to_csv('.../Agg_transaction.csv')


path=".../pulse-master/data/aggregated/user/country/india/state/"
state_list=os.listdir(path)

col = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [],
       'Count': [], 'Percentage': []}

for i in state_list:
    p1=path+i+"/"
    year=os.listdir(p1)
    for j in year:
        p2=p1+j+"/"
        qtr=os.listdir(p2)
        for k in qtr:
            p3=p2+k
            Data=open(p3,'r')
            D=json.load(Data)
            try:
                for x in D['data']['usersByDevice']:
                    brand=x['brand']
                    count=x['count']
                    percentage = x['percentage']
                    col['Brand'].append(brand)
                    col['Count'].append(count)
                    col['Percentage'].append(percentage)
                    col['State'].append(i)
                    col['Year'].append(j)
                    col['Quarter'].append(int(k.strip('.json')))
            except:
                pass

agg_user=pd.DataFrame(col)
agg_user.to_csv('.../Agg_user.csv')


path=".../pulse-master/data/map/transaction/hover/country/india/state/"
state_list=os.listdir(path)

col = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
       'Count': [], 'Amount': []}

for i in state_list:
    p1=path+i+"/"
    year=os.listdir(p1)
    for j in year:
        p2=p1+j+"/"
        qtr=os.listdir(p2)
        for k in qtr:
            p3=p2+k
            Data=open(p3,'r')
            D=json.load(Data)
            try:
                for x in D['data']['hoverDataList']:
                    district=x['name']
                    count=x['metric'][0]['count']
                    amount = x['metric'][0]['amount']
                    col['District'].append(district)
                    col['Count'].append(count)
                    col['Amount'].append(amount)
                    col['State'].append(i)
                    col['Year'].append(j)
                    col['Quarter'].append(int(k.strip('.json')))
            except:
                pass

map_transaction=pd.DataFrame(col)
map_transaction.to_csv('.../map_transaction.csv')

path=".../pulse-master/data/map/user/hover/country/india/state/"
state_list=os.listdir(path)

col = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
       'Registered_users': [], 'App_opens': []}

for i in state_list:
    p1=path+i+"/"
    year=os.listdir(p1)
    for j in year:
        p2=p1+j+"/"
        qtr=os.listdir(p2)
        for k in qtr:
            p3=p2+k
            Data=open(p3,'r')
            D=json.load(Data)
            try:
                for x in D['data']['hoverData']:
                    district=x
                    registered_user=D['data']['hoverData'][x]['registeredUsers']
                    app_opens = D['data']['hoverData'][x]['appOpens']
                    col['District'].append(district)
                    col['Registered_users'].append(registered_user)
                    col['App_opens'].append(app_opens)
                    col['State'].append(i)
                    col['Year'].append(j)
                    col['Quarter'].append(int(k.strip('.json')))
            except:
                pass

map_user=pd.DataFrame(col)
map_user.to_csv('.../map_user.csv')



#Inserting the data from CSV file to SQL database

import pandas as pd
import mysql.connector

# Connect to the MySQL database
db = mysql.connector.connect(
    host='localhost',
    user='*****',
    password='*****',
    database='*****'
)

# Create a cursor object to execute SQL queries
cursor = db.cursor()

# Read the CSV file into a pandas DataFrame
agg_tran_data = pd.read_csv('.../Agg_transaction.csv')
agg_user_data=pd.read_csv('.../Agg_user.csv')
map_tran_data = pd.read_csv('.../map_transaction.csv')
map_user_data= pd.read_csv('.../map_user.csv')

#1st table
# # Iterate over each row in the DataFrame
for index, row in agg_tran_data.iterrows():
    query = "INSERT INTO agg_transact (state,year,quarter,tran_type,tran_count,tran_amount) VALUES (%s, %s,%s,%s,%s,%s)"
    values = (row['State'],row['Year'],row['Quarter'],row['Transaction_type'],row['Transaction_count'],row['Transaction_amount'])
    cursor.execute(query, values)

# Commit the changes to the database
db.commit()

#2nd table

for index, row in map_tran_data.iterrows():
    query = "INSERT INTO map_transact (state,year,quarter,district,count,amount) VALUES (%s,%s,%s,%s,%s,%s)"
    values = (row['State'],row['Year'],row['Quarter'],row['District'],row['Count'],row['Amount'])
    cursor.execute(query, values)

db.commit()

#3rd table

for index, row in agg_user_data.iterrows():
    query = "INSERT INTO agg_user (state,year,quarter,brand,count,percent) VALUES (%s,%s,%s,%s,%s,%s)"
    values = (row['State'],row['Year'],row['Quarter'],row['Brand'],row['Count'],row['Percentage'])
    cursor.execute(query, values)

db.commit()

#4th table

for index, row in map_user_data.iterrows():
    query = "INSERT INTO map_user (state,year,quarter,district,reg_users,app_opens) VALUES (%s,%s,%s,%s,%s,%s)"
    values = (row['State'],row['Year'],row['Quarter'],row['District'],row['Registered_users'],row['App_opens'])
    cursor.execute(query, values)

db.commit()

#Close the cursor and database connection
cursor.close()
db.close()