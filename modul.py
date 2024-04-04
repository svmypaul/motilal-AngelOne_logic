from logzero import logger
import pandas as pd
import time
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import sys
from datetime import datetime
import xlwings as xw
import os
import re
import threading
import requests
from io import StringIO


def add_row_to_dataframe(dataframe, new_row_dict):
    
    # Get the current index count to determine the index for the new row
    index = len(dataframe)
    
    # Use loc to add the new row at the end
    dataframe.loc[index] = new_row_dict
    
    return dataframe


# =============================================================================
# auth_token = AUTH_TOKEN; api_key = API_KEY; client_code = CLIENT_CODE; feed_token = FEED_TOKEN; correlation_id = CORRELATION_ID; mode = MODE; token_list = TOKEN_LIST
# =============================================================================

def fetch_index_CMP(auth_token, api_key, client_code, feed_token, correlation_id, mode, token_list):
    v = []
    
    def on_data(wsapp, message):
        last_traded_price = message.get('last_traded_price')
        if last_traded_price is not None:
            v.append(last_traded_price)
            logger.info("Last Traded Price: {}".format(last_traded_price))
            close_connection()
            
        else:
            logger.warning("No 'last_traded_price' found in the message: {}".format(message))

    def on_control_message(wsapp, message):
        logger.info(f"Control Message: {message}")

    def on_open(wsapp):
        logger.info("on open")
        some_error_condition = False
        if some_error_condition:
            error_message = "Simulated error"
            if hasattr(wsapp, 'on_error'):
                wsapp.on_error("Custom Error Type", error_message)
        else:
            sws.subscribe(correlation_id, mode, token_list)

    def on_error(wsapp, error):
        logger.error(error)

    def on_close(wsapp):
        logger.info("Close")

    def close_connection():
        sws.close_connection()

    def wait_for_exit():
        # Wait for 5 seconds
        time.sleep(1)

        # Close the connection
        close_connection()

        # Exit the script
        sys.exit()
    
    sws = SmartWebSocketV2(auth_token, api_key, client_code, feed_token, max_retry_attempt=0, retry_strategy=0, retry_delay=10, retry_duration=30)
    
    # Assign the callbacks.
    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close
    sws.on_control_message = on_control_message
       
    # Connect to WebSocket
    sws.connect()
    
    return v[-1]


def find_strike_prices(broker ,df, moti_df, market_price, expiry_date, exch_seg, name,gap):
    #df['strike'] = df['strike'].astype(float)
    
    # Find lower and upper strike prices
    lower_strike = df[(df['strike'] <= market_price) &
                      (df['expiry'] == expiry_date) &
                      (df['exch_seg'] == exch_seg) &
                      (df['name'] == name)]['strike'].max()
    
    upper_strike = df[(df['strike'] >= market_price) & 
                      (df['expiry'] == expiry_date) &
                      (df['exch_seg'] == exch_seg) & 
                      (df['name'] == name)]['strike'].min()
    
    if broker == "angleone":
        
        # Retrieve the corresponding rows
        lower_strike_rows = df[(df['strike'] == lower_strike) &
                               (df['expiry'] == expiry_date) &
                               (df['exch_seg'] == exch_seg) &
                               (df['name'] == name)]
        
        # If there are multiple rows for the lower strike, filter by 'CE' or 'PE'
        call_lower_strike_rows = lower_strike_rows[lower_strike_rows['symbol'].str.contains('CE')]
        put_lower_strike_rows = lower_strike_rows[lower_strike_rows['symbol'].str.contains('PE')]
        
        # Now you can access the 'symbol' values for call and put separately
        call_lower_strike_symbol = call_lower_strike_rows['symbol'].values[0] if not call_lower_strike_rows.empty else None
        put_lower_strike_symbol = put_lower_strike_rows['symbol'].values[0] if not put_lower_strike_rows.empty else None
        
        # Now you can access the 'TOKEN' values for call and put separately
        call_lower_strike_token = call_lower_strike_rows['token'].values[0] if not call_lower_strike_rows.empty else None
        put_lower_strike_token = put_lower_strike_rows['token'].values[0] if not put_lower_strike_rows.empty else None
        
        # Retrieve the corresponding rows for upper strike
        upper_strike_rows = df[(df['strike'] == upper_strike) & 
                               (df['expiry'] == expiry_date) & 
                               (df['exch_seg'] == exch_seg) & 
                               (df['name'] == name)]
        
        # If there are multiple rows for the upper strike, filter by 'CE' or 'PE'
        call_upper_strike_rows = upper_strike_rows[upper_strike_rows['symbol'].str.contains('CE')]
        put_upper_strike_rows = upper_strike_rows[upper_strike_rows['symbol'].str.contains('PE')]
        
        # Now you can access the 'symbol' values for call and put separately
        call_upper_strike_symbol = call_upper_strike_rows['symbol'].values[0] if not call_upper_strike_rows.empty else None
        put_upper_strike_symbol = put_upper_strike_rows['symbol'].values[0] if not put_upper_strike_rows.empty else None
    
        # Now you can access the 'token' values for call and put separately
        call_upper_strike_token = call_upper_strike_rows['token'].values[0] if not call_upper_strike_rows.empty else None
        put_upper_strike_token = put_upper_strike_rows['token'].values[0] if not put_upper_strike_rows.empty else None

    if broker == "motilal":
        
        
        # Retrieve the corresponding rows
        call_lower_strike_rows = moti_df[(moti_df['strike'] == int(lower_strike)) &
                               (moti_df['expiry'] == expiry_date) &
                               (moti_df['exch_seg'] == moti_convert_exchangeType(exch_seg)) &
                               (moti_df['name'] == name) & 
                               (moti_df['optiontype'] == "CE")]
        
        put_lower_strike_rows = moti_df[(moti_df['strike'] == lower_strike) &
                               (moti_df['expiry'] == expiry_date) &
                               (moti_df['exch_seg'] == moti_convert_exchangeType(exch_seg)) &
                               (moti_df['name'] == name) & 
                               (moti_df['optiontype'] == "PE")]
        
        call_upper_strike_rows = moti_df[(moti_df['strike'] == upper_strike) &
                               (moti_df['expiry'] == expiry_date) &
                               (moti_df['exch_seg'] == moti_convert_exchangeType(exch_seg)) &
                               (moti_df['name'] == name) & 
                               (moti_df['optiontype'] == "CE")]
        
        put_upper_strike_rows = moti_df[(moti_df['strike'] == upper_strike) &
                               (moti_df['expiry'] == expiry_date) &
                               (moti_df['exch_seg'] == moti_convert_exchangeType(exch_seg)) &
                               (moti_df['name'] == name) & 
                               (moti_df['optiontype'] == "PE")]
        
        call_lower_strike_token = call_lower_strike_rows['token'].values[0]
        put_lower_strike_token = put_lower_strike_rows['token'].values[0]
        
        call_upper_strike_token = call_upper_strike_rows['token'].values[0]
        put_upper_strike_token = put_upper_strike_rows['token'].values[0]
        
        call_lower_strike_symbol = ""
        put_lower_strike_symbol = ""
        
        call_upper_strike_symbol = ""
        put_upper_strike_symbol = ""
        
    lower_strike_token = [call_lower_strike_token, put_lower_strike_token]
    
    lower_strike_symbol = [call_lower_strike_symbol, put_lower_strike_symbol]
    
    upper_strike_token = [call_upper_strike_token, put_upper_strike_token]
    
    upper_strike_symbol = [call_upper_strike_symbol, put_upper_strike_symbol]
   
    if market_price in [lower_strike, lower_strike+15]:
        return lower_strike_token , lower_strike_symbol ,lower_strike , lower_strike
    elif market_price in [upper_strike, upper_strike-15]:
        return upper_strike_token , upper_strike_symbol ,upper_strike , upper_strike
    else:
        return [upper_strike_token[0], lower_strike_token[1]], [upper_strike_symbol[0], lower_strike_symbol[1]], upper_strike, lower_strike

# =============================================================================
# def find_strike_prices(df, market_price, expiry_date, exch_seg, name,gap):
#     # Convert the column to floats
#     df['strike'] = df['strike'].astype(float)
# 
#     # Find lower and upper strike prices
#     lower_strike = df[(df['strike'] <= market_price) &
#                       (df['expiry'] == expiry_date)
#                      & (df['exch_seg'] == exch_seg)
#                      & (df['name'] == name)]['strike'].max()
#     lower_strike_int = int(lower_strike/100)
#     upper_strike = df[(df['strike'] >= market_price) & (df['expiry'] == expiry_date)
#                      & (df['exch_seg'] == exch_seg) & (df['name'] == name)]['strike'].min()
#     upper_strike_int = int(upper_strike/100)
# 
#     # convert 14FEB2024 to 14FEB24
#     parsed_date = datetime.strptime(expiry_date, "%d%b%Y")
#     formatted_date = parsed_date.strftime("%d%b%y").upper()
#     
#     lower_call_symbol = f"{name}{formatted_date}{lower_strike_int}CE"
#     lower_put_symbol = f"{name}{formatted_date}{lower_strike_int}PE"
#     
#     upper_call_symbol = f"{name}{formatted_date}{upper_strike_int}CE"
#     upper_put_symbol = f"{name}{formatted_date}{upper_strike_int}PE"
#     # Retrieve the corresponding rows
#     lower_call_token = df[df['symbol'] == lower_call_symbol]['token'].values[0]
#     lower_put_token = df[df['symbol'] == lower_put_symbol]['token'].values[0]
#     upper_call_token = df[df['symbol'] == upper_call_symbol]['token'].values[0]
#     upper_put_token = df[df['symbol'] == upper_put_symbol]['token'].values[0]
#     
#     
#     lower_strike_token = [lower_call_token,lower_put_token]
#     
#     lower_strike_symbol = [lower_call_symbol,lower_put_symbol]
#     
#     upper_strike_token = [upper_call_token,upper_put_token]
#     
#     upper_strike_symbol = [upper_call_symbol,upper_put_symbol]
#    
#     if market_price in [lower_strike, lower_strike+15]:
#         return lower_strike_token , lower_strike_symbol ,lower_strike , lower_strike
#     elif market_price in [upper_strike, upper_strike-15]:
#         return upper_strike_token , upper_strike_symbol ,upper_strike , upper_strike
#     else:
#         return [upper_strike_token[0], lower_strike_token[1]], [upper_strike_symbol[0], lower_strike_symbol[1]], upper_strike, lower_strike
# 
# =============================================================================
def convert_date(input_date):
    # Convert input date string to datetime object
    input_datetime = datetime.strptime(str(input_date), "%Y-%m-%d")
    #input_datetime = datetime.strptime(str(input_date), "%Y-%m-%d %H:%M:%S")

    # Format the datetime object as 'DDMMMYYYY'
    output_date = input_datetime.strftime("%d%b%Y").upper()

    return output_date

# def convert_date_v2(input_date_series):
#     # Extract the first element from the Series (assuming it's a single value)
#     input_date = input_date_series

#     # Convert input date string to datetime object
#     input_datetime = datetime.strptime(str(input_date), "%Y-%m-%d")

#     # Format the datetime object as 'DDMMMYYYY'
#     output_date = input_datetime.strftime("%d%b%Y").upper()

#     return output_date


def get_order_details(orderid, smartApi, Mofsl, broker):
    if orderid != "":
        if broker == "angelone":
            while True:
                try:
                    time.sleep(0.2)
                    OrderBook = smartApi.orderBook()['data']
                    
                    orderbook_df = pd.DataFrame(OrderBook)
                    df_index = orderbook_df[orderbook_df['orderid'] == str(orderid)].index
                    value = orderbook_df.iloc[df_index[0]]
                    value['tradeprice'] == ""
                    #value = {'orderstatus': "complete","status": "complete",'uniqueorderid': "",'text':"Successfull",   'tradeprice': ""}
                    break
                except Exception as e:
                    logger.warning(f"Get details failed: {e}")
                    time.sleep(5)
        if broker == "motilal":
           while True:
               try:
                   time.sleep(0.2)
                   value = Mofsl.GetOrderDetailByUniqueorderID(orderid)['data'][0]
                   
                   tradebook_price = moti_tradebook(Mofsl=Mofsl,uniqueorderid = orderid)
                   
                   value = {'orderstatus': moti_output_convert(value['orderstatus']),"status": moti_output_convert(value['orderstatus']),'uniqueorderid': orderid,'text': value['error'], 'tradeprice': tradebook_price}
                   # value = {'orderstatus': "complete","status": "complete",'uniqueorderid': "",'text':"Successfull" , 'tradeprice': ""}
                   break
               except Exception as e:
                   logger.warning(f"Get details failed: {e}")
                   time.sleep(5) 
    else:
        value = {'orderstatus': "rejected","status": "rejected",'uniqueorderid': "",'text':"request time out",  'tradeprice': ""}
        
    return value

def get_order_details_v2(orderid, smartApi, Mofsl, broker):
    if orderid != "":
        if broker == "angelone":
            while True:
                try:
                    time.sleep(0.2)
                    OrderBook = smartApi.orderBook()['data']
                    
                    orderbook_df = pd.DataFrame(OrderBook)
                    df_index = orderbook_df[orderbook_df['orderid'] == str(orderid)].index
                    value = orderbook_df.iloc[df_index[0]]
                    value['tradeprice'] = ""
                    # value = {'orderstatus': "trigger pending","status": "trigger pending",'uniqueorderid': "",'text':"Successfull",  'tradeprice': ""}
                    break
                except Exception as e:
                    logger.warning(f"Get details failed: {e}")
                    time.sleep(5)
                
        if broker == "motilal":
           while True:
               try:
                   time.sleep(0.2)
                   
                   value = Mofsl.GetOrderDetailByUniqueorderID(orderid)['data'][0]
                   tradebook_price = moti_tradebook(Mofsl=Mofsl,uniqueorderid = orderid)
                   
                   value = {'orderstatus': moti_output_convert(value['orderstatus']),"status": moti_output_convert(value['orderstatus']),'uniqueorderid': orderid,'text': value['error'], 'tradeprice': tradebook_price}
                   # value = {'orderstatus': "complete","status": "complete",'uniqueorderid': "",'text':"Successfull" , 'tradeprice': ""}
                   break
               except Exception as e:
                   logger.warning(f"Get details failed: {e}")
                   time.sleep(5) 
    else:
        value = {'orderstatus': "rejected","status": "rejected",'uniqueorderid': "",'text':"request time out",  'tradeprice': ""}
        
    return value

    
def place_order(broker,Mofsl, smartApi, variety, Script, token, Status, exchange,ordertype, price, quantity,stoploss_price,num_retry,producttype,lotsize):
    
    if broker == "angelone":
        for retry in range(num_retry) :
            try:
                time.sleep(0.2)
                orderparams = {
                    "variety": variety,
                    "tradingsymbol": Script,
                    "symboltoken": int(token),
                    "transactiontype": Status,
                    "exchange": exchange,
                    "ordertype": ordertype,
                    "producttype": producttype,
                    "duration": "DAY",
                    "price": str(float(price)/100.0),
                    "squareoff": "0",
                    "stoploss": 0,
                    "quantity": str(quantity)
                    }
                # Method 1: Place an order and return the order ID
                #orderid = smartApi.placeOrder(orderparams)
                #logger.info(f"PlaceOrder : {orderid}")
                # Method 2: Place an order and return the full response
                response = smartApi.placeOrderFullResponse(orderparams)
                orderid = response['data']['orderid']
                logger.info(f"PlaceOrder : {response}")
                break
            except Exception as e:
                orderid = ""
                logger.error(f"Order placement failed: {e}")
                time.sleep(2)
    
    if broker == "motilal":
        for retry in range(num_retry) :
            try:
                time.sleep(0.2)
                # PlaceOrderInfo
                Orderinfo = {  
                    "exchange":moti_convert_exchangeType(exchange), # NSEFO, BSEFO,BSE,NSE
                    "symboltoken":int(token), # Exchange Scrip code or Symbol Token is unique identifier
                    "buyorsell":Status, # BUY, SELL
                    "ordertype":'LIMIT', # LIMIT, MARKET, STOPLOSS
                    "producttype":variety, # NORMAL, DELIVERY, SELLFROMDP, VALUEPLUS, BTST,MTF
                    "orderduration":"DAY", # DAY,GTC,GTD,IOC
                    "price":int(float(price)/100.0), # 
                    "triggerprice":0,
                    "quantityinlot":int(lotsize),
                    "disclosedquantity":0,
                    "amoorder":"N", # Y or N
                    "algoid":"", # Algo Id or Blank for Non-Algo Orders
                    "tag":" " # Echo back to identify order
                    }
                orderinfo =  Mofsl.PlaceOrder(Orderinfo)
                #orderinfo['status'] = moti_output_convert(orderinfo['status'])
                print(orderinfo)
                
                orderid = orderinfo['uniqueorderid']
                logger.info(f"PlaceOrder : {orderinfo}")
                break
            except Exception as e:
                orderid = ""
                logger.error(f"Order placement failed: {e}")
                time.sleep(2)
                
    return orderid
            

def sl_place_order(smartApi,broker,Mofsl, variety, Script, token, Status, exchange,ordertype, price, quantity, orderid, triggerprice ,num_retry, producttype,lotsize):
    
    if broker == "angelone":
        for retry in range(num_retry) :
            try:
                time.sleep(0.2)
                orderparams = {
                    "variety": variety,
                    "tradingsymbol": Script,
                    "symboltoken": int(token),
                    "transactiontype": Status,
                    "exchange": exchange,
                    "orderid": orderid,
                    "ordertype": ordertype,
                    "producttype": producttype,
                    "duration": "DAY",
                    "price": str(float(price)/100.0),
                    "triggerprice": str(float(triggerprice)/100.0),
                    "quantity": str(quantity)
                    }
                # Method 1: Place an order and return the order ID
                #orderid = smartApi.placeOrder(orderparams)
                #logger.info(f"PlaceOrder : {orderid}")
                # Method 2: Place an order and return the full response
                response = smartApi.placeOrderFullResponse(orderparams)
                orderid = response['data']['orderid']
                logger.info(f"PlaceOrder : {response}")
                break
            except Exception as e:
                orderid = ""
                logger.error(f"Order placement failed: {e}")
                time.sleep(2)
                
    if broker == "motilal":
        
        for retry in range(num_retry) :
            # try:
            time.sleep(0.2)
            # PlaceOrderInfo
            Orderinfo = {  
                "exchange":moti_convert_exchangeType(exchange), # NSEFO, BSEFO,BSE,NSE
                "symboltoken":int(token), # Exchange Scrip code or Symbol Token is unique identifier
                "buyorsell":Status, # BUY, SELL
                "ordertype":variety, # LIMIT, MARKET, STOPLOSS
                "producttype":'NORMAL', # NORMAL, DELIVERY, SELLFROMDP, VALUEPLUS, BTST,MTF
                "orderduration":"DAY", # DAY,GTC,GTD,IOC
                "price":float(price)/100.0, # 
                "triggerprice":float(triggerprice)/100.0,
                "quantityinlot":int(lotsize),
                "disclosedquantity":0,
                "amoorder":"N", # Y or N
                "algoid":"", # Algo Id or Blank for Non-Algo Orders
                "tag":" " # Echo back to identify order
                }
            orderinfo =  Mofsl.PlaceOrder(Orderinfo)
            #orderinfo['status'] = moti_output_convert(orderinfo['status'])
            print(orderinfo)
            
            orderid = orderinfo['uniqueorderid']
            logger.info(f"PlaceOrder : {orderinfo}")
            break
            # except Exception as e:
            #     orderid = ""
            #     logger.error(f"Order placement failed: {e}")
            #     time.sleep(2)
        
    return orderid        

        
def extract_trade_type(trade_id):
    return trade_id.split('_')[1]

def cancel_order(smartApi, orderid, variety):
    can = smartApi.cancelOrder(orderid, varitey = variety)

# Function to convert decimal to time
def decimal_to_time(decimal_value):
    hours, remainder = divmod(decimal_value * 24, 1)
    minutes, seconds = divmod(remainder * 60, 1)

    return f"{int(hours):02d}:{int(minutes):02d}"

# =============================================================================
def update_excel(df, excel_file_path, sheet_name="Sheet1"):
    # save df as csv
    file_name = excel_file_path.split('.xlsx')[0]
    csv_file_path = f"{file_name}.csv"
    df.to_csv(csv_file_path,index = False)
    # Open the existing Excel workbook
# =============================================================================
#     wb = xw.Book(excel_file_path)
#     ws = wb.sheets[sheet_name]
#     
#     # Clear existing data in the sheet
#     ws.clear()
# 
#     # Write the DataFrame to the Excel sheet
#     ws["A1"].options(pd.DataFrame, header=1, index=False, expand='table').value = df
# 
#     # Format column names and index
#     ws["A1"].expand("right").api.Font.Bold = True
#     ws["A1"].expand("right").api.Borders.Weight = 2
# 
#     # Save the changes to the workbook
#     wb.save()
# =============================================================================
    
    
    # Close the workbook
    #wb.close()

def get_exchangeType(val):
    if val == "NFO":
        value = 2
    if val == "BFO":
        value = 4
    return value


def extract_name(input_v):
    # Split based on numeric characters
    parts = re.split(r'(\d+)', input_v)

    # Remove empty strings from the list
    parts = parts[0]
    return parts

def check_producttype(config_df):
    
    # Get the values in the 'order_type' column
    order_types = config_df['producttype']

    # Check if all values are either INTRADAY or CARRYFORWARD
    if not all(order_type in ['carryforward', 'intraday'] for order_type in order_types):
        logger.error("Error: 'producttype' column must contain only 'carryforward' or 'intraday'.")
        raise SystemExit
        
    # If all values are valid, convert them to uppercase
    config_df['producttype'] = config_df['producttype'].str.upper()
    
    
def round_to_nearest_tick_size(price):
    price = float(price)
    if round(price % 0.05,3) != 0.05:
        price = round(price - (price % 0.05),3)  
        
    return str(price)


def check_insufficient_funds(text):
    # Check if the text contains "Insufficient Funds"
    if "Insufficient Funds" in text or "Insufficient" in text:
        return True
    else:
        return False

    
#---------------------------Motilal_Special_Modul----------------------

def moti_convert_exchangeType(exctyp):
    
    if exctyp == 1:
        value = "NSE"
        
    elif exctyp == 3:
        value = "BSE"
        
    elif exctyp == 2 or exctyp == "NFO":
        value = "NSEFO"
        
    elif exctyp == 4 or exctyp == "BFO":
        value = "BSEFO"
        
    else:
        value = exctyp
        
    return(value)

def moti_fetch_data(url):
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
    
def fetch_CMP(broker, Mofsl, auth_token, api_key, client_code, feed_token, correlation_id, mode, token_list):
    if broker == "angleone":
        v = []
        
        def on_data(wsapp, message):
            last_traded_price = message.get('last_traded_price')
            if last_traded_price is not None:
                v.append(last_traded_price)
                logger.info("Last Traded Price: {}".format(last_traded_price))
                close_connection()
                
            else:
                logger.warning("No 'last_traded_price' found in the message: {}".format(message))

        def on_control_message(wsapp, message):
            logger.info(f"Control Message: {message}")

        def on_open(wsapp):
            logger.info("on open")
            some_error_condition = False
            if some_error_condition:
                error_message = "Simulated error"
                if hasattr(wsapp, 'on_error'):
                    wsapp.on_error("Custom Error Type", error_message)
            else:
                sws.subscribe(correlation_id, mode, token_list)

        def on_error(wsapp, error):
            logger.error(error)

        def on_close(wsapp):
            logger.info("Close")

        def close_connection():
            sws.close_connection()

        def wait_for_exit():
            # Wait for 5 seconds
            time.sleep(1)

            # Close the connection
            close_connection()

            # Exit the script
            sys.exit()
        
        sws = SmartWebSocketV2(auth_token, api_key, client_code, feed_token, max_retry_attempt=0, retry_strategy=0, retry_delay=10, retry_duration=30)
        
        # Assign the callbacks.
        sws.on_open = on_open
        sws.on_data = on_data
        sws.on_error = on_error
        sws.on_close = on_close
        sws.on_control_message = on_control_message
           
        # Connect to WebSocket
        sws.connect()
        
        return v[-1]
    
    if broker == "motilal":
        exch = moti_convert_exchangeType(token_list[0]['exchangeType'])
        token = token_list[0]['tokens'][0]
        LTPData = {
            "clientcode":"",
            "exchange":exch,
            "scripcode":int(token)
        }
    
        ltpdata = Mofsl.GetLtp(LTPData)  
        return ltpdata['data']['ltp']

def moti_convert_date_format(date_str):
    
  day = date_str[:2]
  month = date_str[2:5].upper()
  year = date_str[5:]
  
  return f"{day}-{month}-{year}"

def moti_format_number(v):
    formatted_number = str(round(v/100,1))
    if formatted_number.endswith('.0'):
        formatted_number = int(float(formatted_number))
    return formatted_number

def moti_generate_scripname(name, expiry_date, option_type, strike):
    
    expiry_date = moti_convert_date_format(expiry_date)
    strike = moti_format_number(strike)

    scripname = f"{name} {expiry_date} {option_type} {strike}"
    
    return scripname

def moti_extract_and_format_date(text):
    pattern = r'(\d{2})-(\w{3})-(\d{4})'
    matches = re.findall(pattern, text)
    formatted_date = ''.join(matches[0])
    return formatted_date.upper()

def moti_output_convert(text):
    if text == "Error":
        output = "rejected"
        
    elif text == "Traded":
        output = "complete"
    
    elif text == "Confirm":
        output = 'trigger pending'
        
    elif text == "Sent":
        output = 'trigger pending'
    
    else:
        output = text
    return output

def moti_tradebook(Mofsl,uniqueorderid):
    try:
        tradebook = Mofsl.GetTradeBook("")['data']
        for i in range(len(tradebook)):
            if tradebook[i]['uniqueorderid'] == uniqueorderid :
                return(tradebook[i]['tradeprice'])
            else:
                pass
    except:
        return("")

def moti_netqty(buyquantity, sellquantity):
    if buyquantity == sellquantity :
        netqty = 0
    
    elif buyquantity < sellquantity:
        
        netqty = sellquantity - buyquantity
    
    else:
        
        netqty = buyquantity - sellquantity
    
    return netqty


def fetch_lotsize(symbol, data):
    
    index_name = symbol.split(' ')[0]
    
    filtered_data = data[data['motilal_trade_name'].str.upper() == index_name]
    
    lots = filtered_data['Lot']
    
    return lots