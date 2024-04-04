# -*- coding: utf-8 -*-
"""
Created on Fri Feb 16 13:02:37 2024

@author: SVMY
"""

import os
os.chdir(r"C:\Users\SVMY\Downloads\index_trade_v2\index_trade_motilal_v0.2") 

from MOFSLOPENAPI import MOFSLOPENAPI
import pyotp

# You will get Your api key from website 
ApiKey = "JVSmbjZIYDv5WLlh" 


# userid and password is your trading account username and password
userid = "ANKN0040" 
password = "Qwert@1357"   
Two_FA = "01/05/1951"
vendorinfo = "ANKN0040"
clientcode = None 

# if Your SourceId is web then pass browsername and browser version in case of Desktop you dont need to passanyting

SourceID = "Desktop"            # Web,Desktop
browsername = "chrome"      
browserversion = "104"      
totp_key = "GHBFD55F3T4XX2VEXN3FTQAYWSGUDCV6"
time_otp = pyotp.TOTP(totp_key)
totp = time_otp.now()

Base_Url = "https://openapi.motilaloswal.com"


# Initialize MofslOpenApi using Apikey, Base_Url and clientcode 
Mofsl = MOFSLOPENAPI(ApiKey, Base_Url, clientcode, SourceID, browsername, browserversion)

print(Mofsl.login(userid, password, Two_FA, totp, vendorinfo))

# def Broadcast_on_open(ws1):
#     Mofsl.Register("NSEFO", "DERIVATIVES", 53944)
    
#     #Mofsl.Broadcast_Logout()

# def Broadcast_on_message(ws1, message_type, message):
    
    

#     if(message_type == "LTP"):
#         print(message)


# def Broadcast_on_close(ws1, close_status_code, close_msg):
#     # print("########Broadcast_closed########")
#     # print("Broadcast Connection Closed")
#     print("Close Message : %s" %(close_msg))
#     print("Close Message Code : %s" %(close_status_code)) 

# # Assign the callbacks.
# Mofsl._Broadcast_on_open = Broadcast_on_open
# Mofsl._Broadcast_on_message = Broadcast_on_message
# Mofsl._Broadcast_on_close = Broadcast_on_close

# Mofsl.Broadcast_new() # to start
# Mofsl.Broadcast_connect() # to run
# Mofsl.Broadcast_Logout() # to close



import threading
import time

def Broadcast_on_open(ws1):
    Mofsl.Register("NSEFO", "DERIVATIVES", 53944)

def Broadcast_on_message(ws1, message_type, message):
    if message_type == "LTP":
        print(message['LTP_Rate'])
        if 'LTP_Rate' in message:
            if isinstance(message['LTP_Rate'], (int, float)):
                global stop_thread
                stop_thread = True

# Assign the callbacks.
Mofsl._Broadcast_on_open = Broadcast_on_open
Mofsl._Broadcast_on_message = Broadcast_on_message

# Define a function to run the broadcast in a thread
def run_broadcast():
    Mofsl.Broadcast_new() # to start
    Mofsl.Broadcast_connect() # to run

stop_thread = False

# Define a function to run the broadcast in a separate thread
def run_broadcast_thread():
    global stop_thread
    stop_thread = False
    thread = threading.Thread(target=run_broadcast)
    thread.start()
    while not stop_thread:
        time.sleep(1)
    print("Thread stopped.")
    Mofsl.Broadcast_Logout()

# Start the broadcast in a separate thread
run_broadcast_thread()


cmp_price = fetch_CMP("auth_token", "api_key", "client_code", "feed_token", "correlation_id", "mode", "token_list",Mofsl=Mofsl)

TOKEN_LIST = [{"exchangeType": int(trade_data.iloc[i]['exchangeType']), "tokens": [tokv.values[0]]}]

index_CMP =fetch_CMP('AUTH_TOKEN', 'API_KEY', 'CLIENT_CODE', 'FEED_TOKEN', 'CORRELATION_ID', 'MODE', TOKEN_LIST)


    
import requests
import pandas as pd
from io import StringIO

def fetch_data(url):
    # Make the GET request to the API
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Read the CSV data into a DataFrame using StringIO to simulate a file object
        df = pd.read_csv(StringIO(response.text))
        return df
    else:
        print('Failed to fetch data. Status code:', response.status_code)
        return None

# URLs for NSE , BSE, NSEFO amd BSEFO data
nse_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=NSE'
bse_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=BSE'
nsefo_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=NSEFO'
bsefo_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=BSEFO'

# Fetch data for NSE , BSE, NSEFO amd BSEFO
df_nse = fetch_data(nse_url)
df_bse = fetch_data(bse_url)
df_nsefo = fetch_data(nsefo_url)
df_bsefo = fetch_data(bsefo_url)

# Concatenate the DataFrames
if df_nse is not None and df_bse is not None:
    df_combined = pd.concat([df_nse, df_bse, df_bsefo, df_nsefo], ignore_index=True)
    print(df_combined.head())


symboltoken = 26009
exchangetype = "NSE"

def Broadcast_on_open(ws1):
    Mofsl.Register("NSE", "CASH", 45855)

def Broadcast_on_message(ws1, message_type, message):
    if message_type == "LTP":
        print(message)
        #v.append(message['LTP_Rate'])
        if 'LTP_Rate' in message:
            if isinstance(message['LTP_Rate'], (int, float)):
                global stop_thread
                stop_thread = True

# Assign the callbacks.
Mofsl._Broadcast_on_open = Broadcast_on_open
Mofsl._Broadcast_on_message = Broadcast_on_message

# Define a function to run the broadcast in a thread
def run_broadcast():
    Mofsl.Broadcast_new() # to start
    Mofsl.Broadcast_connect() # to run

stop_thread = False

# Define a function to run the broadcast in a separate thread
def run_broadcast_thread():
    global stop_thread
    stop_thread = False
    thread = threading.Thread(target=run_broadcast)
    thread.start()
    while not stop_thread:
        time.sleep(1)
    print("Thread stopped.")
    Mofsl.Broadcast_Logout()

# Start the broadcast in a separate thread
run_broadcast_thread()

def TCPBroadcast_on_open():
    # print("########TCPBroadcast_Opened########")
    # print("AuthValidate after connection opened")

    # Exchange -BSE, NSE, NSEFO, MCX, NSECD, NCDEX, BSEFO
    # Exchange Type- CASH,DERIVATIVES   Scrip Code-eg 532540

    Mofsl.TCPRegister("NSE", "CASH", 45855)
    # Mofsl.TCPRegister("BSE", "CASH", 532540)
    # Mofsl.TCPRegister("MCX", "DERIVATIVES", 245470)
    # Mofsl.TCPRegister("NSEFO", "DERIVATIVES",55917)
    # Mofsl.TCPRegister("NCDEX", "DERIVATIVES",59259)
    # Mofsl.TCPRegister("BSEFO", "DERIVATIVES",873973)

    # Mofsl.TCPUnRegister("BSE", "CASH", 532540)


    # Index BSE, NSE
    # Mofsl.TCPIndexRegister("NSE")
    # Mofsl.TCPIndexRegister("BSE")

    # Mofsl.TCPIndexUnregister("NSE")
    # Mofsl.TCPIndexUnregister("BSE")

    # TCPBroadcast Logout
    # Mofsl.TCPBroadcast_Logout()

def TCPBroadcast_on_message(message_type, message):
    
    if message_type == "Index":
        print(message)

    elif(message_type == "LTP"):
        print(message)
    elif(message_type == "MarketDepth"):
        print(message)
    elif(message_type == "DayOHLC"):
        print(message)
    elif(message_type == "DPR"):
        print(message)
    elif(message_type == "OpenInterest"):
        print(message)

    else:
        print(message)
    pass


    # print(message, message_type)
    



# Assign the callbacks.
Mofsl._TCPBroadcast_on_open = TCPBroadcast_on_open
Mofsl._TCPBroadcast_on_message = TCPBroadcast_on_message


# # Connect 
Mofsl.TCPBroadcast_connect()

LTPData = {
    "clientcode":clientcode,
    "exchange":"BSEFO",
    "scripcode":828981
}

# Mofsl.GetLtp(LTPData)   
print(Mofsl.GetLtp(LTPData))

from nsetools import Nse

# Get live data for an index
nse = Nse()
index_symbol = "NIFTY"  # Replace with desired index symbol
index_data = nse.get_index_quote(index_symbol)

# Extract live price
live_price = index_data["lastPrice"]

print(f"Live price of {index_symbol}: {live_price}")
Nse.get_index_quote(,code="nifty bank")
Nse.get_index_list()
from nsetools import Nse

from nsepy.live import get_quote

def fetch_index_price(index_code):
    index_data = get_quote(index_code)
    if index_data:
        return index_data['lastPrice']
    return None

if __name__ == "__main__":
    index_code = "NIFTY 50"
    index_price = fetch_index_price(index_code)
    if index_price:
        print(f"The live price of {index_code} is {index_price}")
    else:
        print("Index price not found")
get_quote("NIFTY")


moti_df[moti_df['scripname'] == moti_generate_scripname("CUBL","25APR2024","CE",14250)]



moti_generate_scripname("CUBL","25APR2024","CE",14250)

# Test cases
v1 = 120740.0
v2 = round(127500.54,1)

print(format_number(v1))  # Output: 1200
print(format_number(v2))  # Output: 1200.5

import datetime

# Assuming the epoch timestamp is stored in a variable called 'epoch_time'
epoch_time = 	1394928000
  # Example epoch timestamp

# Convert epoch time to a datetime object
date_time = datetime.datetime.fromtimestamp(epoch_time)

# Print the date and time in a readable format
print("Date and Time:", date_time)

# If you only want to print the date in a specific format
formatted_date = date_time.strftime("%Y-%m-%d")
print("Formatted Date:", formatted_date)

import pandas as pd
import re

# Sample DataFrame
data = {'Column': ['CUBL 25-APR-2024 CE 142.5', 'XYZ 12-DEC-2023 AB 345.6']}
df = pd.DataFrame(data)

# Regular expression pattern to match the date format


# Function to extract and format date


# Apply the function to all rows of the column
moti_df['expiry'] = moti_df['scripname'].apply(moti_extract_and_format_date)

print(df)
moti_extract_and_format_date(moti_df['scripname'][0])

find_strike_prices(broker = "motilal" ,df = moti_df, market_price = 70080015, expiry_date = '15MAR2024', exch_seg = 'BSE' , name ="BSX",gap = 1000)


broker = "motilal"

market_price=index_CMP
expiry_date= convert_date(trade_data.iloc[i]['Expiry_date'])
exch_seg=trade_data.iloc[i]['exch_seg_']
name =trade_data.iloc[i]['Probable Trade'].upper()
gap =trade_data.iloc[i]['gap']


LTPData = {
    "clientcode":None,
    "exchange":'NSEFO',
    "scripcode":43966
}

Mofsl.GetLtp(LTPData)

LTPData = {
    "clientcode":clientcode,
    "exchange":"BSE",
    "scripcode":500317
}

# Mofsl.GetLtp(LTPData)   
print(Mofsl.GetLtp(LTPData))

# PlaceOrderInfo
Orderinfo = {      
    "exchange":moti_convert_exchangeType("NFO"),
    "symboltoken":strike_token[0],
    "buyorsell":"SELL",
    "ordertype":"LIMIT",
    "producttype":"NORMAL",
    "orderduration":"DAY",
    "price":price,
    "triggerprice":0,
    "quantityinlot":1,
    "disclosedquantity":0,
    "amoorder":"N",
    "algoid":"",
    "tag":" "
}

# Mofsl.PlaceOrder(Orderinfo)
print(Mofsl.ModifyOrder(OrderInfo))

# PlaceOrderInfo
Orderinfo ={
    "exchange":'NSEFO', # NSEFO, BSEFO,BSE,NSE
    "symboltoken":41345, # Exchange Scrip code or Symbol Token is unique identifier
    "buyorsell":'SELL', # BUY, SELL
    "ordertype":'STOPLOSS', # LIMIT, MARKET, 
    "producttype":'NORMAL', # NORMAL, DELIVERY, SELLFROMDP, VALUEPLUS, BTST,MTF
    "orderduration":"DAY", # DAY,GTC,GTD,IOC
    "price":290, # 
    "triggerprice":330,
    "quantityinlot":15,
    "disclosedquantity":0,
    "amoorder":"N", # Y or N
    "algoid":"", # Algo Id or Blank for Non-Algo Orders
    "tag":" " # Echo back to identify order
}

# Mofsl.PlaceOrder(Orderinfo)
print(Mofsl.PlaceOrder(Orderinfo))

Orderinfo ={
    "exchange":moti_convert_exchangeType(exchange), # NSEFO, BSEFO,BSE,NSE
    "symboltoken":int(token), # Exchange Scrip code or Symbol Token is unique identifier
    "buyorsell":'BUY', # BUY, SELL
    "ordertype":'STOPLOSS', # LIMIT, MARKET, 
    "producttype":'NORMAL', # NORMAL, DELIVERY, SELLFROMDP, VALUEPLUS, BTST,MTF
    "orderduration":"DAY", # DAY,GTC,GTD,IOC
    "price":float(triggerprice)/100.0, # 
    "triggerprice":float(triggerprice)/100.0-5.0,
    "quantityinlot":int(lotsize),
    "disclosedquantity":0,
    "amoorder":"N", # Y or N
    "algoid":"", # Algo Id or Blank for Non-Algo Orders
    "tag":" " # Echo back to identify order
}

# Mofsl.PlaceOrder(Orderinfo)
print(Mofsl.PlaceOrder(Orderinfo))

# PlaceOrderInfo

orderinfo = {
"clientcode":"", 
"uniqueorderid ":"1200057ANKN1226",
"newordertype":"STOPLOSS",
"neworderduration":"DAY",
"newquantityinlot":50,
"newdisclosedquantity":0,
"newprice":186,
"newtriggerprice":180,
"newgoodtilldate":"",
"lastmodifiedtime": "12-Mar-2024 13:35:44",
"qtytradedtoday": 50
}



print(Mofsl.ModifyOrder(orderinfo))

while True:
    v = Mofsl.GetOrderDetailByUniqueorderID('0300024ANKN1226')['data'][0]['orderstatus']
    print(v)
    time.sleep(10)
    
OrderBookInfo = {
    "clientcode":"",
    "dateandtime":""        #22-Dec-2022 15:16:02
}
# Mofsl.GetOrderBook(OrderBookInfo)   
print(Mofsl.GetOrderBook(OrderBookInfo))

# Mofsl.GetTradeBook(clientcode)   
print(Mofsl.GetTradeBook()['data'])
get_order_details(orderid = '1100024ANKN1226' , smartApi = smartApi, Mofsl = Mofsl, broker = 'motilal')
# # orderid Will be recovered from orderbook
# # Mofsl.CancelOrder(orderid, clientcode)   
print(Mofsl.CancelOrder("2600001T024312"))

OrderBookInfo = {
    "clientcode":"",
    "dateandtime":''      #22-Dec-2022 15:16:02
}
# Mofsl.GetOrderBook(OrderBookInfo)   
print(Mofsl.GetOrderBook(OrderBookInfo)['data'])

# You will get Your api key from website 
ApiKey = "JVSmbjZIYDv5WLlh" 


# userid and password is your trading account username and password
userid = "ANKN0040" 
password = "Qwert@1357"   
Two_FA = "01/05/1951"
vendorinfo = "ANKN0040" 
clientcode = ""

# if Your SourceId is web then pass browsername and browser version in case of Desktop you dont need to passanyting

SourceID = "Web"            # Web,Desktop
browsername = "chrome"      
browserversion = "104"      
totp_key = "GHBFD55F3T4XX2VEXN3FTQAYWSGUDCV6"
time_otp = pyotp.TOTP(totp_key)
totp = time_otp.now()
Base_Url = "https://uatopenapi.motilaloswal.com"


# Initialize MofslOpenApi using Apikey, Base_Url and clientcode 
Mofsl = MOFSLOPENAPI(ApiKey, Base_Url, clientcode, SourceID, browsername, browserversion)

# Mofsl.login(userid, password)
print(Mofsl.login(userid, password, Two_FA, totp, vendorinfo))

Mofsl.GetPosition(clientcode)

# Assuming moti_df['name'] contains the 'motilal_trade_name' values

moti_trade_name = trade_data[['Probable Trade', 'motilal_trade_name']]

# Filtering out non-empty and non-null values
filtered_trade_name = moti_trade_name[(moti_trade_name['motilal_trade_name'] != "") & (moti_trade_name['motilal_trade_name'].notna())]

# Convert DataFrame to dictionary
result = filtered_trade_name.set_index('motilal_trade_name')['Probable Trade'].str.upper().to_dict()

# If you want to convert the dictionary to the specified format {"1":"o","j":"7",....}
replace_dict = {str(k): str(v) for k, v in result.items()}

moti_df['name'] = moti_df['name'].replace(replace_dict)
ApiKey
Mofsl.GetPosition(clientcode)['data'][2]
jeson = [{'buyqty': 0, 'sellqty': 2}, {'buyqty': 1, 'sellqty': 0}]

for item in jeson:
    item['netqty'] = item['buyqty'] + item['sellqty']

print(jeson)




 OrderBookInfo = {
    "clientcode":"",
    "dateandtime":""        #22-Dec-2022 15:16:02
}
orderbook =  Mofsl.GetOrderBook(OrderBookInfo)   
tradebook = Mofsl.GetTradeBook(clientcode)['data']
uniqueorderid = "2600062ANKN1226"
for i in range(len(orderbook['data'])):
    if tradebook[i]['uniqueorderid'] == uniqueorderid :
        print(tradebook[i]['tradeprice'])
    else:
        pass


tradebook[0]['uniqueorderid']

def fetch_lotsize (symbol):
    index_name = symbol.split(' ')[0]
    
    filtered_data = trade_data[trade_data['motilal_trade_name'].str.upper() == index_name]
    
    lots = filtered_data['Lot']
    
    return lots