import os
os.chdir(r"C:\Users\SVMY\Downloads\index_trade_v2\motilal+angel1_v0.4")  # replace with your own directory


from SmartApi import SmartConnect 
from logzero import logger
import pandas as pd
import time
import requests
from datetime import datetime
import pytz
from modul import fetch_CMP, find_strike_prices, convert_date, add_row_to_dataframe ,decimal_to_time, sl_place_order, round_to_nearest_tick_size, check_insufficient_funds
from modul import place_order , cancel_order, get_order_details ,extract_trade_type, update_excel,  get_exchangeType, get_order_details_v2, extract_name, check_producttype
import pyotp
import xlwings as xw
import warnings
from MOFSLOPENAPI import MOFSLOPENAPI
from modul import moti_fetch_data, moti_extract_and_format_date,fetch_index_CMP,fetch_lotsize

# Ignore all warnings
warnings.filterwarnings("ignore")

#-------------------------select your broker name-----------------------------------

broker_name = "motilal"  # motilal or angelone


## config data modification and filter

config_df = pd.read_excel("config.xlsx")
config_df = config_df[config_df['active'] != False]
config_df['Expiry_date'] = [ val.strftime("%Y-%m-%d") for val in config_df['Expiry_date'] ]

try:
    check_producttype(config_df)
    
except ValueError as e:
    print(e)

#------------------------------Smart-api-login-----------------------------------

# login
api_key = 'EeaXUllA'  #'YHDjc5SL' # 'EeaXUllA'
username = 'EEQU1010' #'EEQU1081' # 'EEQU1010'
pwd = '1234'  #'3001' #'1234'
totp_key = 'LZ5MP435UT5KWFRHACROBAMJI4' #'2ENUKDECYPBJPOO76E3MHHBS2E' # 'LZ5MP435UT5KWFRHACROBAMJI4'

smartApi = SmartConnect(api_key)

try:
    time_otp = pyotp.TOTP(totp_key)
    time_otp = time_otp.now()
    data = smartApi.generateSession(username, pwd, time_otp)
except Exception as e:
    logger.error("Invalid Token: The provided token is not valid.")
    raise e

correlation_id = "abcde"


if data['status'] == True:
    authToken = data['data']['jwtToken']
    feedToken = smartApi.getfeedToken()
    logger.info("Smart-api login Successful")
else:
    raise Exception(data['message'])

AUTH_TOKEN = authToken
API_KEY = api_key
CLIENT_CODE = username
FEED_TOKEN = feedToken
CORRELATION_ID = "abc123"
action = 1
MODE = 1

#--------------------------------motilal_login------------------------
trade = "live"

if trade == "live":
    # You will get Your api key from website 
    ApiKey = "uaV313NvRC37Ga8V"  #"VGOL0ODb6NjJz31O" #  #   #    # # 
    
    
    # userid and password is your trading account username and password
    userid = "ANKN1226"   #"ANKN0040"   #  # #   #  #  
    password = "asdf@1234"  #"Qwert@1357"  #  # #    #    # 
    Two_FA = "12/07/2021"   #"01/05/1951"   #    # #   #    # 
    vendorinfo = "ANKN1226"  #"ANKN0040"    #    # #  #   #  
    clientcode = None 
    
    # if Your SourceId is web then pass browsername and browser version in case of Desktop you dont need to passanyting
    
    SourceID = "Desktop"            # Web,Desktop
    browsername = "chrome"      
    browserversion = "104"      
    totp_key =  "7DMPPWYHF75KKH7NAQRQKF3FTXDATOI7"    # "GHBFD55F3T4XX2VEXN3FTQAYWSGUDCV6"   #  #"7DMPPWYHF75KKH7NAQRQKF3FTXDATOI7"   #      
    time_otp = pyotp.TOTP(totp_key)
    totp = time_otp.now()
    
    Base_Url = "https://openapi.motilaloswal.com"
    #Base_Url = "https://uatopenapi.motilaloswal.com"
    
    # Initialize MofslOpenApi using Apikey, Base_Url and clientcode 
    Mofsl = MOFSLOPENAPI(ApiKey, Base_Url, clientcode, SourceID, browsername, browserversion)
    
    
    login = Mofsl.login(userid, password, Two_FA, totp, vendorinfo)
if trade == "test":
    # You will get Your api key from website 
    ApiKey = "VGOL0ODb6NjJz31O"  # #  #   #    # # 
    
    
    # userid and password is your trading account username and password
    userid = "ANKN0040"   #   #  # #   #  #  
    password = "Qwert@1357"  #  #  # #    #    # 
    Two_FA = "01/05/1951"   #   #    # #   #    # 
    vendorinfo = "ANKN0040"  #    #    # #  #   #  
    clientcode = None 
    
    # if Your SourceId is web then pass browsername and browser version in case of Desktop you dont need to passanyting
    
    SourceID = "Desktop"            # Web,Desktop
    browsername = "chrome"      
    browserversion = "104"      
    totp_key =  "GHBFD55F3T4XX2VEXN3FTQAYWSGUDCV6"   #    #  #"7DMPPWYHF75KKH7NAQRQKF3FTXDATOI7"   #      
    time_otp = pyotp.TOTP(totp_key)
    totp = time_otp.now()
    
    Base_Url = "https://openapi.motilaloswal.com"
    #Base_Url = "https://uatopenapi.motilaloswal.com"
    
    # Initialize MofslOpenApi using Apikey, Base_Url and clientcode 
    Mofsl = MOFSLOPENAPI(ApiKey, Base_Url, clientcode, SourceID, browsername, browserversion)
    
    
    login = Mofsl.login(userid, password, Two_FA, totp, vendorinfo)
if login['status'] == 'SUCCESS':
    logger.info(f"Motilal login: {login['status']} Message: {login['message']}")
else:
    logger.error(f"Motilal login: {login['status']} Message: {login['message']}")


# print(data)
## following code is to fectch token

# -----------------------------------Motilal_token---------------------------------
# API endpoint URL
# URLs for NSE , BSE, NSEFO amd BSEFO data
# nse_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=NSE'
# bse_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=BSE'
nsefo_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=NSEFO'
bsefo_url = 'https://openapi.motilaloswal.com/getscripmastercsv?name=BSEFO'

# Fetch data for NSE , BSE, NSEFO amd BSEFO
# df_nse = moti_fetch_data(nse_url)
# df_bse = moti_fetch_data(bse_url)
df_nsefo = moti_fetch_data(nsefo_url)
df_bsefo = moti_fetch_data(bsefo_url)

# Concatenate the DataFrames
if df_nsefo is not None and df_bsefo is not None:
    moti_df = pd.concat([df_bsefo, df_nsefo], ignore_index=True)
    
    moti_df['expiry'] = moti_df['scripname'].apply(moti_extract_and_format_date)
    moti_df.rename(columns={'strikeprice': 'strike', 'scripshortname': 'name', 'exchangename': 'exch_seg','scripcode' : 'token'}, inplace=True)
    
    moti_df['strike'] = moti_df['strike'].astype(float)
    
    
    moti_df['strike'] *= 100
    
    moti_df.to_csv("moti_token.csv", index=False)


# -----------------------Smart Api_token----------------------------------------------
# API endpoint URL
url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'

# Make the GET request to the API
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    json_data = response.json()
    
    # Convert JSON to DataFrame
    df = pd.DataFrame(json_data)
    df['strike'] = df['strike'].astype(float)

#----------------------Edit Data Structure----------------------
formatted_day = datetime.now().strftime("%A")
trade_data = config_df[config_df['Day']==formatted_day]
trade_data.reset_index(inplace = True, drop=True)

if broker_name == "motilal" :

    moti_trade_name = trade_data[['Probable Trade', 'motilal_trade_name']]
    
    # Filtering out non-empty and non-null values
    filtered_trade_name = moti_trade_name[(moti_trade_name['motilal_trade_name'] != "") & (moti_trade_name['motilal_trade_name'].notna())]
    
    # Convert DataFrame to dictionary
    result = filtered_trade_name.set_index('motilal_trade_name')['Probable Trade'].str.upper().to_dict()
    

    replace_dict = {str(k): str(v) for k, v in result.items()}
    
    moti_df['name'] = moti_df['name'].replace(replace_dict)

    
    

#-----------------read previous track data--------------------

file_name = "trade_track" # Replace this with the actual file name

excel_file_path = f"{file_name}.xlsx" 
csv_file_path = f"{file_name}.csv"  


if os.path.exists(csv_file_path):
    # If the CSV file exists, read it into a DataFrame
    trade_df = pd.read_csv(csv_file_path)

    
    
else:
    # If the CSV file doesn't exist, create an empty DataFrame with the specified columns
    columns = ["date", "Trade_id", 'name', "trade_type", "token", "time", "exchange", "tradingsymbol", "quantity",
               "strike_price", "market_price", "trigger_price", "num_sl_hits", "status", "trns_id", "gap",
               "expiry_date","unique_order_id","text",'producttype','lot']
    trade_df = pd.DataFrame(columns=columns)
    
    

# Now you can use the 'trade_df' DataFrame for further processing


kolkata_timezone = pytz.timezone('Asia/Kolkata')

date = datetime.now(kolkata_timezone).date().strftime("%Y-%m-%d")


# Filter trade_df based on the current date

trade_df = trade_df[trade_df['date'] == date]


#------------------------Below is the code for automation---------------------------

trade_indicator = False
fresh_trade_indicator = True


while True:
    
    try:
        current_time = datetime.now(kolkata_timezone).strftime('%H:%M')
        
        print( datetime.now(kolkata_timezone) )
        
        # no of index
        for i in range(len(trade_data)):
            try:
                starting_time_index = str(trade_data.iloc[i]['Start Time'])[0:5]
                
                print(i)
                if '15:08' >= current_time >= starting_time_index:
                #if True:
                    
                    trade_indicator = False
                    
                    if int(current_time[3:5]) % int(trade_data.iloc[i]['Frequency']) == 0 and fresh_trade_indicator:
                    #if int(current_time[3:5]) % 2 == 0:
                        
                        current_time = str(current_time)
                        trade_indicator = True
                        
                        print( trade_data.iloc[i]['Probable Trade'].upper() )
            
                        tokv = df[(df['name']==trade_data.iloc[i]['Probable Trade'].upper()) & (df['exch_seg']==trade_data.iloc[i]['exch_seg'])]['token']
                        # index CMP
                        TOKEN_LIST = [{"exchangeType": int(trade_data.iloc[i]['exchangeType']), "tokens": [tokv.values[0]]}]
                        
                        index_CMP =fetch_index_CMP(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN, CORRELATION_ID, MODE, TOKEN_LIST)
                        
                        strike_token, strike_symbols, call_strike, put_strike = find_strike_prices(broker_name, df, moti_df, index_CMP, convert_date(trade_data.iloc[i]['Expiry_date']) , trade_data.iloc[i]['exch_seg_'], trade_data.iloc[i]['Probable Trade'].upper(), trade_data.iloc[i]['gap'])
                        
                        ## 
                        if len(strike_token) == 2 and len(strike_symbols) == 2 and put_strike is not None and call_strike is not None:
                            print(strike_token, strike_symbols, call_strike, put_strike)
                            
                            # time.sleep(0.2)
                            TOKEN_LIST = [{"exchangeType": int(trade_data.iloc[i]['exchangeType_']), "tokens": [str(strike_token[0])]}]
                            
                            o_call_CMP =fetch_CMP(broker = broker_name, Mofsl = Mofsl , auth_token = AUTH_TOKEN, api_key = API_KEY, client_code = CLIENT_CODE, feed_token = FEED_TOKEN, correlation_id = CORRELATION_ID, mode = MODE, token_list = TOKEN_LIST)
                            # time.sleep(0.2)
                            TOKEN_LIST = [{"exchangeType": int(trade_data.iloc[i]['exchangeType_']), "tokens": [strike_token[1]]}]
                            
                            o_put_CMP =fetch_CMP(broker = broker_name, Mofsl = Mofsl , auth_token = AUTH_TOKEN, api_key = API_KEY, client_code = CLIENT_CODE, feed_token = FEED_TOKEN, correlation_id = CORRELATION_ID, mode = MODE, token_list = TOKEN_LIST)
                            # time.sleep(0.2)
                            stoploss_price = float(o_call_CMP) + float(o_put_CMP)
                            
                            #option call sell
                            
                            call_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = strike_symbols[0], token = strike_token[0], Status = "SELL", exchange = trade_data.iloc[i]['exch_seg_'],ordertype = "LIMIT",price = o_call_CMP, quantity = trade_data.iloc[i]['quantity'],stoploss_price = stoploss_price,num_retry = 5 , producttype = trade_data.iloc[i]['producttype'],broker = broker_name,lotsize = trade_data.iloc[i]['Lot'],Mofsl = Mofsl)
                            
                            call_order_details = get_order_details(call_id, smartApi,Mofsl,broker_name)
                            call_trade_price = call_order_details['tradeprice']
                            print(call_order_details)
                            
                            add_row = {"date":str(date),"Trade_id":f"{strike_symbols[0]}_call_0","name":trade_data.iloc[i]['Probable Trade'].upper(),"trade_type":"normal_sell","token":strike_token[0],"time":current_time,"exchange":trade_data.iloc[i]['exch_seg_'],"tradingsymbol":strike_symbols[0],"quantity":trade_data.iloc[i]['quantity'],"strike_price":call_strike,"market_price":float(o_call_CMP)/100.0,"trigger_price":float(o_call_CMP)/100.0,"num_sl_hits":0,"status":call_order_details['orderstatus'],"trns_id":str(call_id),"gap":trade_data.iloc[i]['gap'],"expiry_date":trade_data.iloc[i]['Expiry_date'], "unique_order_id":call_order_details['uniqueorderid'],"text":call_order_details['text'],'producttype':trade_data.iloc[i]['producttype'],'lot': trade_data.iloc[i]['Lot']}
                
                            trade_df = add_row_to_dataframe(trade_df, add_row)
                           
                            update_excel(trade_df,"trade_track.xlsx")
                            
                            # stop fresh trade for insufisiant fund 
                            if check_insufficient_funds(call_order_details['text']):
                                fresh_trade_indicator = False
                            
                            # time.sleep(0.2)

                            if call_order_details['orderstatus'] != "rejected":
                            #if True:
                                # option put sell
                                
                                put_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = strike_symbols[1], token = strike_token[1],Status = "SELL", exchange = trade_data.iloc[i]['exch_seg_'],ordertype = "LIMIT",price = o_put_CMP, quantity = trade_data.iloc[i]['quantity'],stoploss_price = stoploss_price,num_retry = 5 , producttype = trade_data.iloc[i]['producttype'],broker = broker_name,lotsize = trade_data.iloc[i]['Lot'],Mofsl = Mofsl)
                                
                                put_order_details = get_order_details(put_id, smartApi, Mofsl, broker_name)
                                put_trade_price = put_order_details['tradeprice']
                                
                                add_row = {"date":date,"Trade_id":f"{strike_symbols[1]}_put_0",'name':trade_data.iloc[i]['Probable Trade'].upper(),"trade_type":"normal_sell","token":strike_token[1],"time":current_time,"exchange":trade_data.iloc[i]['exch_seg_'],"tradingsymbol":strike_symbols[1],"quantity":trade_data.iloc[i]['quantity'],"strike_price":put_strike, "market_price":float(o_put_CMP)/100.0,"trigger_price":float(o_put_CMP)/100.0,"num_sl_hits":0,"status":put_order_details['orderstatus'], "trns_id":str(put_id),"gap":trade_data.iloc[i]['gap'],"expiry_date":trade_data.iloc[i]['Expiry_date'], "unique_order_id":put_order_details['uniqueorderid'],"text":put_order_details['text'],'producttype':trade_data.iloc[i]['producttype'],'lot': trade_data.iloc[i]['Lot']}
                                
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                                
                                update_excel(trade_df,"trade_track.xlsx")
                                # time.sleep(0.2)
                                
                                if check_insufficient_funds(put_order_details['text']):
                                    fresh_trade_indicator = False
                            else:
                                put_order_details = {'orderstatus': "rejected","status": "rejected",'uniqueorderid': "",'text':"call order rejected"}
                            
                            if put_order_details['orderstatus'] != "rejected" and call_order_details['orderstatus'] != "rejected":
                                
                                # if broker_name == "motilal":
                                #     stoploss_price = float(call_trade_price) + float(put_trade_price)
                                # else:
                                #     pass
                                
                                #option call stoploss
                                call_stoploss_trns_id = sl_place_order(smartApi=smartApi, variety = "STOPLOSS", Script = strike_symbols[0], token = strike_token[0],Status = "BUY", exchange = trade_data.iloc[i]['exch_seg_'],ordertype = "STOPLOSS_LIMIT",price = str(int(stoploss_price)+500), quantity = trade_data.iloc[i]['quantity'],triggerprice = str(int(stoploss_price)),orderid = call_id ,num_retry = 10, producttype = trade_data.iloc[i]['producttype'],broker = broker_name,lotsize = trade_data.iloc[i]['Lot'],Mofsl = Mofsl)
                                
                                logger.info(f"price: {str(int(stoploss_price)+500)} and  tigger price: {str(int(stoploss_price))}")
                                
                                sl_order_details = get_order_details_v2(call_stoploss_trns_id, smartApi,Mofsl,broker_name)
                                
                                add_row = {"date":date,"Trade_id":f"{strike_symbols[0]}_call_SL_0","name":trade_data.iloc[i]['Probable Trade'].upper(),"trade_type":"stoploss_buy","token":strike_token[0],"time":current_time,"exchange":trade_data.iloc[i]['exch_seg_'],
"tradingsymbol":strike_symbols[0],"quantity":trade_data.iloc[i]['quantity'],"strike_price":call_strike,"market_price":float(int(stoploss_price)+500)/100.0,"trigger_price":float(stoploss_price)/100.0,"num_sl_hits":0,"status":sl_order_details['orderstatus'], "trns_id":str(call_stoploss_trns_id),"gap":trade_data.iloc[i]['gap'],"expiry_date":trade_data.iloc[i]['Expiry_date'], "unique_order_id":sl_order_details['uniqueorderid'],"text":sl_order_details['text'],'producttype':trade_data.iloc[i]['producttype'],'lot': trade_data.iloc[i]['Lot']}
                    
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                                
                                update_excel(trade_df,"trade_track.xlsx")
                                
                                #option put stoploss
                                put_stoploss_trns_id = sl_place_order(smartApi=smartApi, variety = "STOPLOSS",Script = strike_symbols[1],token = strike_token[1],Status = "BUY",exchange = trade_data.iloc[i]['exch_seg_'],ordertype = "STOPLOSS_LIMIT", price = str(int(stoploss_price)+500),quantity = trade_data.iloc[i]['quantity'], triggerprice =  str(int(stoploss_price)),orderid = put_id, num_retry = 10, producttype = trade_data.iloc[i]['producttype'],broker = broker_name,lotsize = trade_data.iloc[i]['Lot'],Mofsl = Mofsl)
                                
                                logger.info(f"price: {str(int(stoploss_price)+500)} and  tigger price: {str(int(stoploss_price))}")
                                
                                sl_order_details = get_order_details_v2(put_stoploss_trns_id, smartApi,Mofsl,broker_name)
                                
                                add_row = {"date":date,"Trade_id":f"{strike_symbols[1]}_put_SL_0",'name':trade_data.iloc[i]['Probable Trade'].upper(),"trade_type":"stoploss_buy","token":strike_token[1],"time":current_time,"exchange":trade_data.iloc[i]['exch_seg_'],
"tradingsymbol":strike_symbols[1],"quantity":trade_data.iloc[i]['quantity'],"strike_price":put_strike,
"market_price":float(int(stoploss_price)+500)/100.0,"trigger_price":float(stoploss_price)/100.0,"num_sl_hits":0,"status":sl_order_details['orderstatus'],"trns_id":str(put_stoploss_trns_id),"gap":trade_data.iloc[i]['gap'],"expiry_date":trade_data.iloc[i]['Expiry_date'], "unique_order_id":sl_order_details['uniqueorderid'],"text":sl_order_details['text'],'producttype':trade_data.iloc[i]['producttype'],'lot': trade_data.iloc[i]['Lot']}
                    
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                                
                                update_excel(trade_df,"trade_track.xlsx")
                            
                            # If unable to sell put, buy a call option with the same amount
                            if put_order_details['orderstatus'] == "rejected" and call_order_details['orderstatus'] != "rejected" :
                                
                                # if broker_name == "motilal":
                                #     stoploss_price = float(call_trade_price) + float(put_trade_price)
                                # else:
                                #     pass
                                
                                call_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = strike_symbols[0], token = strike_token[0], Status = "BUY", exchange = trade_data.iloc[i]['exch_seg_'],ordertype = "MARKET",price = "0", quantity = trade_data.iloc[i]['quantity'],stoploss_price = stoploss_price,num_retry = 5 , producttype = trade_data.iloc[i]['producttype'],broker = broker_name,lotsize = trade_data.iloc[i]['Lot'],Mofsl = Mofsl)
                                
                                Call_order_details = get_order_details(call_id, smartApi,Mofsl,broker_name)
                                
                                add_row = {"date":str(date),"Trade_id":f"{strike_symbols[0]}_call_0","name":trade_data.iloc[i]['Probable Trade'].upper(),"trade_type":"normal_buy","token":strike_token[0],"time":current_time,"exchange":trade_data.iloc[i]['exch_seg_'],"tradingsymbol":strike_symbols[0],"quantity":trade_data.iloc[i]['quantity'], "strike_price":call_strike,"market_price":float(o_call_CMP)/100.0,"trigger_price":float(o_call_CMP)/100.0,"num_sl_hits":0,"status":Call_order_details['orderstatus'], "trns_id":str(call_id),"gap":trade_data.iloc[i]['gap'],"expiry_date":trade_data.iloc[i]['Expiry_date'], "unique_order_id":Call_order_details['uniqueorderid'],"text":Call_order_details['text'],'producttype':trade_data.iloc[i]['producttype'],'lot': trade_data.iloc[i]['Lot']}
                    
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                               
                                update_excel(trade_df,"trade_track.xlsx")
                                
                            # If unable to sell call, buy a put option with the same amount
                            if call_order_details['orderstatus'] == "rejected" and put_order_details['orderstatus'] != "rejected" :
                                
                                # if broker_name == "motilal":
                                #     stoploss_price = float(call_trade_price) + float(put_trade_price)
                                # else:
                                #     pass
                                
                                put_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = strike_symbols[1], token = strike_token[1], Status = "BUY", exchange = trade_data.iloc[i]['exch_seg_'],ordertype = "MARKET", price = "0", quantity = trade_data.iloc[i]['quantity'],stoploss_price = stoploss_price,num_retry = 5, producttype = trade_data.iloc[i]['producttype'],broker = broker_name,lotsize = trade_data.iloc[i]['Lot'],Mofsl = Mofsl)
                                
                                Put_order_details = get_order_details(put_id, smartApi,Mofsl,broker_name)
                                add_row = {"date":date,"Trade_id":f"{strike_symbols[1]}_put_0",'name':trade_data.iloc[i]['Probable Trade'].upper(),"trade_type":"normal_buy","token":strike_token[1],"time":current_time,"exchange":trade_data.iloc[i]['exch_seg_'],
"tradingsymbol":strike_symbols[1],"quantity":trade_data.iloc[i]['quantity'],"strike_price":put_strike,
"market_price":float(o_put_CMP)/100.0,"trigger_price":float(o_put_CMP)/100.0,"num_sl_hits":0,"status":Put_order_details['orderstatus'],
"trns_id":str(put_id),"gap":trade_data.iloc[i]['gap'],"expiry_date":trade_data.iloc[i]['Expiry_date'], "unique_order_id":Put_order_details['uniqueorderid'],"text":Put_order_details['text'],'producttype':trade_data.iloc[i]['producttype'],'lot': trade_data.iloc[i]['Lot']}
                                
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                                
                                update_excel(trade_df,"trade_track.xlsx")
                        
                        else:
                            print("strike_token, strike_symbols, call_strike, put_strike not found")
                            logger.error("strike_token, strike_symbols, call_strike, put_strike not found")
            except Exception as e:
                # Handle other exceptions (if any)
                logger.error(f"An error occurred: {e}")
                
                
                    
        # cancel all pending orders or selling orders        
        if "15:08" < current_time < "15:15":
            print("cancel all")
            try:
                current_time = datetime.now(kolkata_timezone).strftime('%H:%M')
                print( datetime.now(kolkata_timezone) )
                
                if broker_name =="angelone":
                    netpos = smartApi.position()
                    #netpos = read_json_file("data.txt")
    
                
                    if netpos['data'] != None:
                        for o in netpos['data']:
                            
                            print(int(o['netqty']))
                            if int(o['netqty']) != 0:
                                transactiontype = "SELL" if int(o['netqty']) > 0 else "BUY"
                                logger.info(f"{smartApi}, variety = 'NORMAL', Script = {o['tradingsymbol']}, token = {o['symboltoken']}, Status = {transactiontype} , exchange = {o['exchange']}, ordertype = 'MARKET', price = '0', quantity = {abs(int(o['netqty']))}, stoploss_price = '0',num_retry = 5 , producttype = o['producttype']")
                                
                                
                                
                                can_order_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = o['tradingsymbol'], token = o['symboltoken'], Status = transactiontype , exchange = o['exchange'], ordertype = "MARKET", price = "0", quantity = abs(int(o['netqty'])), stoploss_price = "0",num_retry = 5 , producttype = o['producttype'] ,broker = broker_name,lotsize = '',Mofsl = Mofsl)                            
                                
                                
                                
                                order_details = get_order_details(can_order_id, smartApi,Mofsl,broker_name)
                                
                                # add row
                                add_row = {"date":date,"Trade_id": f"{o['tradingsymbol']}_can_buy", 'name': "", "trade_type":  "NORMAL", "token": o['symboltoken'], "time": current_time, "exchange": o['exchange'], "tradingsymbol": o['tradingsymbol'], "quantity": o['netqty'], "strike_price": o['strikeprice'], "market_price": '0', "trigger_price": "0", "num_sl_hits": "", "status": order_details['orderstatus'], "trns_id": can_order_id, "gap": "", "expiry_date": o['expirydate'], "unique_order_id":order_details['uniqueorderid'], "text":order_details['text'],'producttype': o['producttype'],'lot': ''}
                
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                
                                update_excel(trade_df,"trade_track.xlsx")
                
                if broker_name =="motilal":
                    
                    netpos = Mofsl.GetPosition(clientcode)
                    
                    if netpos['data'] != None:
                        for o in netpos['data']:
                            
                            # o['netqty'] = moti_netqty(buyquantity = o['buyquantity'], sellquantity = o['sellquantity'])
                            o['netqty'] = o['buyquantity'] - o['sellquantity']
                            o['lot'] = fetch_lotsize(symbol = o['symbol'], data = config_df)
                            
                            print(int(o['netqty']))
                            if int(o['netqty']) != 0:
                                transactiontype = "SELL" if int(o['netqty']) > 0 else "BUY"
                                logger.info(f"{smartApi}, variety = 'NORMAL', Script = {o['symboltoken']}, token = {o['symboltoken']}, Status = {transactiontype} , exchange = {o['exchange']}, ordertype = 'MARKET', price = '0', quantity = {abs(int(o['netqty']))}, stoploss_price = '0',num_retry = 5 , producttype = o['productname']")
                                
                                
                                
                                can_order_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = o['symboltoken'], token = o['symboltoken'], Status = transactiontype , exchange = o['exchange'], ordertype = "MARKET", price = "0", quantity = abs(int(o['netqty'])), stoploss_price = "0",num_retry = 5 , producttype = o['producttype'] ,broker = broker_name,lotsize = o['lot'],Mofsl = Mofsl)                            
                                
                                
                                order_details = get_order_details(can_order_id, smartApi,Mofsl,broker_name)
                                
                                # add row
                                add_row = {"date":date,"Trade_id": f"{o['symboltoken']}_can_buy", 'name': "", "trade_type":  "NORMAL", "token": o['symboltoken'], "time": current_time, "exchange": o['exchange'], "tradingsymbol": o['symbol'], "quantity": o['netqty'], "strike_price": o['strikeprice'], "market_price": '0', "trigger_price": "0", "num_sl_hits": "", "status": order_details['orderstatus'], "trns_id": can_order_id, "gap": "", "expiry_date": o['expirydate'], "unique_order_id":order_details['uniqueorderid'], "text":order_details['text'],'producttype': o['optiontype'],'lot': o['lot']}
                
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                
                                update_excel(trade_df,"trade_track.xlsx")
                    
                            # time.sleep(0.2)
                if broker_name =="angelone":
                    
                    orderbook = smartApi.orderBook()
            
                    if orderbook['data'] != None:
                        for o in orderbook['data']:
                            if o['orderstatus'] == "open" or o['orderstatus'] == 'trigger pending' or o['orderstatus'] == 'validation pending' or o['orderstatus'] == 'open pending' or o['orderstatus'] == 'AMD SUBMITTED':
                            
                                
                                print("orderbook")
                                # place cancel order
                                can = smartApi.cancelOrder(o['orderid'], o['variety'])
                                print(can['data'])
                                
                                order_details = get_order_details(o['orderid'], smartApi,Mofsl,broker_name)
                                # add row
                                add_row = {"date":date,"Trade_id": f"{o['tradingsymbol']}_can", 'name': extract_name(o['tradingsymbol']), "trade_type": o['variety'],"token": o['symboltoken'], "time": current_time, "exchange": o['exchange'], "tradingsymbol": o['tradingsymbol'], "quantity": o['quantity'], "strike_price": o['strikeprice'],"market_price": o['price'], "trigger_price": o['triggerprice'], "num_sl_hits": "","status": order_details['orderstatus'],"trns_id": can['data']['orderid'], "gap": "", "expiry_date": o['expirydate'], "unique_order_id":order_details['uniqueorderid'],"text":order_details['text'],'producttype':o['producttype'],'lot':''}
                
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                
                                update_excel(trade_df,"trade_track.xlsx")
                
                if broker_name == "motilal":
                    OrderBookInfo = {
                        "clientcode":"",
                        "dateandtime":''      #22-Dec-2022 15:16:02
                    }
                    orderbook = Mofsl.GetOrderBook(OrderBookInfo)
                    
                    if orderbook['data'] != None:
                        for o in orderbook['data']:
                            if o['orderstatus'] == "open" or o['orderstatus'] == 'Confirm' or o['orderstatus'] == 'validation pending' or o['orderstatus'] == 'open pending' or o['orderstatus'] == 'AMD SUBMITTED':
                            
                                
                                print("orderbook")
                                # place cancel order
                                can = Mofsl.CancelOrder(o['uniqueorderid'])
                                print(can['message'])
                                
                                order_details = get_order_details(o['uniqueorderid'], smartApi,Mofsl,broker_name)
                                # add row
                                add_row = {"date":date,"Trade_id": f"{o['symbol']}_can", 'name': extract_name(o['symbol']), "trade_type": o['series'],"token": o['symboltoken'], "time": current_time, "exchange": o['exchange'], "tradingsymbol": o['symbol'], "quantity": o['orderqty'], "strike_price": o['strikeprice'],"market_price": o['price'], "trigger_price": o['triggerprice'], "num_sl_hits": "","status": can['status'],"trns_id": o['uniqueorderid'], "gap": "", "expiry_date": o['expirydate'], "unique_order_id":o['uniqueorderid'],"text":can['message'],'producttype':o['producttype'],'lot':''}
                
                                trade_df = add_row_to_dataframe(trade_df, add_row)
                                
                                update_excel(trade_df,"trade_track.xlsx")
                    
                        # time.sleep(0.2)
            except Exception as e:
                # Handle other exceptions (if any)
                logger.error(f"An error occurred: {e}")
                pass
        
        
        # stop trading at 15:15
        # if current_time > "15:15":
        #     break
         
        #if int(current_time[3:5]) % int(trade_data.iloc[i]['Frequency']) == 0:
        #if int(current_time[3:5]) % 2 == 0:
        #i =1   
        current_time = datetime.now(kolkata_timezone).strftime('%H:%M')
        
        if trade_indicator and ( i == len(trade_data) - 1 ) and ( int(current_time[3:5]) % int(trade_data.iloc[i]['Frequency']) == 0 ):
        #if trade_indicator and ( i == len(trade_data) - 1 ) and ( int(current_time[3:5]) % 2 == 0 ):
            time_to_sleep = 60-datetime.now(kolkata_timezone).second
            print( f"Going to sleep for {time_to_sleep} seconds." )
            time.sleep(time_to_sleep)
        
        
        # check for stoplosses status and prepare for next trade
        if len(trade_df) > 0:
            
            trade_sl_df = trade_df[(trade_df['trade_type'] == 'stoploss_buy') & (trade_df['status'] == 'trigger pending')]
            
            trade_sl_df.reset_index(inplace=True, drop=True)
           
            if len(trade_sl_df) > 0:
                for j in range(len(trade_sl_df)):
                    
                    current_time = datetime.now(kolkata_timezone).strftime('%H:%M')
                    
                    #.... shouldn't go to status check if trading second is triggered ...
                    if (int(current_time[3:5]) % int(trade_data.iloc[i]['Frequency']) == 0) or (current_time >= "15:08"):
                        break;
                    else:
                        if trade_sl_df['num_sl_hits'][j] == 0:
                            sl_status = get_order_details(trade_sl_df['trns_id'][j], smartApi,Mofsl,broker_name)['orderstatus']
                            print(trade_sl_df['trns_id'][j])
                            
                            logger.info("SL Status : {}, Num SL Hits : {}".format(sl_status, trade_sl_df['num_sl_hits'][j] ))
                            
                            if sl_status == 'complete' :
                                
                                
                                
                                # update status
                                trade_df.loc[trade_df['trns_id'] == trade_sl_df['trns_id'][j], 'status'] = 'complete'
    
                                
                                # get trade details
                                exch_seg = trade_df.loc[trade_df['trns_id'] == trade_sl_df['trns_id'][j], 'exchange'].iloc[0]
                                
                                gap = trade_df.loc[trade_df['trns_id'] == trade_sl_df['trns_id'][j], 'gap'].iloc[0]
                                
                                expiry_date = trade_df.loc[trade_df['trns_id'] == trade_sl_df['trns_id'][j], 'expiry_date'].iloc[0]
                                
                                name = trade_df.loc[trade_df['trns_id'] == trade_sl_df['trns_id'][j], 'name'].iloc[0]
                                
                                quantity = trade_df.loc[trade_df['trns_id'] == trade_sl_df['trns_id'][j], 'quantity'].iloc[0]
                                # lot = 1
                                lot = trade_df.loc[trade_df['lot'] == trade_sl_df['lot'][j], 'lot'].iloc[0]
                                producttype = trade_df.loc[trade_df['trns_id'] == trade_sl_df['trns_id'][j], 'producttype'].iloc[0]
            
                                #time_trade_df = trade_df[(trade_df['time'] == trade_sl_df['time'][j]) & (trade_df['trade_type'] == 'stoploss_buy')]
                                time_trade_df = trade_df[(trade_df['time'] == trade_sl_df['time'][j]) & (trade_df['trade_type'] == 'stoploss_buy') & (trade_df['name']==trade_sl_df['name'][j])]
                                
                                time_trade_df.reset_index(inplace = True, drop=True)
                                # apply condition call_stoploss tiggered sell again
                                if extract_trade_type(trade_sl_df['Trade_id'][j]) == "call":
                                    
                                    logger.info("Entering to call after SL")
                                    # prevoius call and put cmp and strik price
                                    prev_call_strik =  time_trade_df.iloc[0]['strike_price']
                                    prev_put_strik =  time_trade_df.iloc[1]['strike_price']
                                    prev_call_cmp =  time_trade_df.iloc[0]['market_price']
                                    prev_put_cmp =  time_trade_df.iloc[1]['market_price']
                                    #next_strike_price = "{:.6f}".format(prev_call_cmp+(3*gap))
                                    next_strike_price = prev_call_strik+(3*gap)
                                    
                                    next_call_trade = df[(df['strike'] == next_strike_price) & (df['expiry'] == convert_date(expiry_date)) & (df['exch_seg'] == exch_seg) & 
                                      (df['name'] == name)]

                                    # Use .loc to set values
                                    next_call_trade['ce_pe'] = next_call_trade['symbol'].str[-2:]
                                    next_call_trade_row = next_call_trade[next_call_trade['ce_pe'] == "CE"]
                                    
                                    
                                    #df[(df['expiry'] ==convert_date(expiry_date))]
                                    # time.sleep(0.2)
                                    # fetch next call cmp
                                    TOKEN_LIST = [{"exchangeType": get_exchangeType(exch_seg), "tokens": [str(next_call_trade_row['token'].iloc[0])]}]
                                    next_o_call_CMP =fetch_CMP(broker = broker_name, Mofsl = Mofsl, auth_token = AUTH_TOKEN, api_key = API_KEY, client_code = CLIENT_CODE, feed_token = FEED_TOKEN, correlation_id = CORRELATION_ID, mode = MODE, token_list = TOKEN_LIST)
                                    # time.sleep(0.2)
                                    stoploss_price = int(float(prev_put_cmp*100) + float(next_o_call_CMP))
                                    # sell next call
                                    next_call_trns_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = next_call_trade_row['symbol'].iloc[0], token = str(next_call_trade_row['token'].iloc[0]), Status = "SELL", exchange = exch_seg,ordertype = "LIMIT", price = next_o_call_CMP, quantity = quantity,stoploss_price = stoploss_price,num_retry = 10, producttype = producttype,broker = broker_name,lotsize = lot,Mofsl = Mofsl)
                                    
                                    order_details = get_order_details(next_call_trns_id, smartApi,Mofsl,broker_name)
                                    Ncall_trade_price = order_details['tradeprice']
                                    
                                    add_row = {"date":date,"Trade_id":f"{next_call_trade_row['symbol'].iloc[0]}_call_1","name":name,"trade_type":"normal_sell","token":str(next_call_trade_row['token'].iloc[0]),"time":current_time,"exchange":exch_seg,
"tradingsymbol":next_call_trade_row['symbol'].iloc[0],"quantity":quantity,"strike_price": prev_call_strik+(3*gap),
"market_price":float(next_o_call_CMP)/100.0,"trigger_price": float(next_o_call_CMP)/100.0,"num_sl_hits":1,"status":order_details['orderstatus'],"trns_id":next_call_trns_id,"gap":gap,"expiry_date":expiry_date,"unique_order_id":order_details['uniqueorderid'],"text":order_details['text'],'producttype': producttype,'lot':lot}
            
                                    trade_df = add_row_to_dataframe(trade_df, add_row)
                                    
                                    update_excel(trade_df,"trade_track.xlsx")
                                    if order_details['orderstatus'] != "rejected":
                                    # add stoploss
                                        # stoploss_price = int(float(prev_put_cmp*100) + float(Ncall_trade_price))
                                        
                                        # time.sleep(0.2)
                                        next_sl_trns_id = sl_place_order(smartApi=smartApi, variety = "STOPLOSS", Script = next_call_trade_row['symbol'].iloc[0], token = str(next_call_trade_row['token'].iloc[0]), Status = "BUY", exchange = exch_seg,ordertype = "STOPLOSS_LIMIT",
price = str(int(stoploss_price)), quantity = quantity,triggerprice = str(int(stoploss_price)-500),orderid = next_call_trns_id,num_retry = 10, producttype = producttype,broker = broker_name,lotsize = lot,Mofsl = Mofsl)
                                        
                                        logger.info(f"price: {str(int(stoploss_price)+500)} and  tigger price: {str(int(stoploss_price))}")
                                        
                                        order_details = get_order_details_v2(next_sl_trns_id, smartApi,Mofsl,broker_name)
                                        
                                        add_row = {"date":date,"Trade_id":f"{next_call_trade_row['symbol'].iloc[0]}_sl_call_1","name":name,"trade_type":"stoploss_buy","token":str(next_call_trade_row['token'].iloc[0]),"time":current_time,"exchange":exch_seg,
"tradingsymbol":next_call_trade_row['symbol'].iloc[0],"quantity":quantity,"strike_price": prev_call_strik+(3*gap),
"market_price":float(int(stoploss_price))/100.0,"trigger_price": float(int(stoploss_price)-500)/100.0,"num_sl_hits":1,"status":order_details['orderstatus'],"trns_id":next_sl_trns_id,"gap":gap,"expiry_date":expiry_date, "unique_order_id":order_details['uniqueorderid'],"text":order_details['text'],'producttype': producttype,'lot':lot}
                
                                        trade_df = add_row_to_dataframe(trade_df, add_row)
                                        
                                        update_excel(trade_df,"trade_track.xlsx")
                                    
                                    
                                    
                                    
                                if extract_trade_type(trade_sl_df['Trade_id'][j]) == "put":
                                    
                                    logger.info("Entering to put after SL")
                                    
                                    # prevoius call and put cmp and strik price
                                    prev_call_strik =  time_trade_df.iloc[0]['strike_price']
                                    prev_put_strik =  time_trade_df.iloc[1]['strike_price']
                                    prev_call_cmp =  time_trade_df.iloc[0]['market_price']
                                    prev_put_cmp =  time_trade_df.iloc[1]['market_price']
                                    
                                    prev_put_cmp = float(prev_put_cmp)
                                    gap = float(gap)
                                    
                                    #next_strike_price = "{:.6f}".format(prev_put_cmp - (3 * gap))
                                    next_strike_price = prev_put_strik + (3 * gap)
                                    
                                    next_put_trade = df[(df['strike'] == next_strike_price) & (df['expiry'] == convert_date(expiry_date)) & (df['name'] == name)]
                                    
                                    # Use .loc to set values
                                    next_put_trade['ce_pe'] = next_put_trade['symbol'].str[-2:]
                                    
                                    next_put_trade_row = next_put_trade[next_put_trade['ce_pe'] == "PE"]

                                    # time.sleep(0.2)
                                    # fetch next put cmp
                                    
                                    
                                    TOKEN_LIST = [{"exchangeType": get_exchangeType(exch_seg), "tokens": [str(next_put_trade_row['token'].iloc[0])]}]
                                    next_o_put_CMP =fetch_CMP(broker = broker_name, Mofsl = Mofsl, auth_token = AUTH_TOKEN, api_key = API_KEY, client_code = CLIENT_CODE, feed_token = FEED_TOKEN, correlation_id = CORRELATION_ID, mode = MODE, token_list = TOKEN_LIST)
                                    
                                    stoploss_price = int(float(prev_call_cmp*100) + float(next_o_put_CMP))
                                    # time.sleep(0.2)
                                    # sell next put
                                    next_put_trns_id = place_order(smartApi=smartApi, variety = "NORMAL", Script = next_put_trade_row['symbol'].iloc[0], token = next_put_trade_row['token'].iloc[0],Status = "SELL", exchange = exch_seg,ordertype = "LIMIT",price = next_o_put_CMP, quantity = quantity,stoploss_price = stoploss_price,num_retry = 10, producttype = producttype,broker = broker_name,lotsize = lot,Mofsl = Mofsl)
                                    
                                    order_details = get_order_details(next_put_trns_id, smartApi,Mofsl,broker_name)
                                    Nput_trade_price = order_details['tradeprice']
                                    
                                    add_row = {"date":date,"Trade_id":f"{next_put_trade_row['symbol'].iloc[0]}_put_1","name":name,"trade_type":"normal_sell","token":str(next_put_trade_row['token'].iloc[0]),"time":current_time,"exchange":exch_seg,
"tradingsymbol":next_put_trade_row['symbol'].iloc[0],"quantity":quantity,"strike_price": prev_put_strik+(3*gap),
"market_price":float(next_o_put_CMP)/100.0,"trigger_price": float(next_o_put_CMP)/100.0,"num_sl_hits":1,"status":order_details['orderstatus'],
"trns_id":next_put_trns_id,"gap":gap,"expiry_date":expiry_date,"unique_order_id":order_details['uniqueorderid'],"text":order_details['text'],'producttype':producttype}
                                    
                                    trade_df = add_row_to_dataframe(trade_df, add_row)
                                    
                                    update_excel(trade_df,"trade_track.xlsx")
                                    # time.sleep(0.1)
                                    if order_details['orderstatus'] != "rejected":
                                        # add stoploss
                                        # stoploss_price = int(float(prev_call_cmp*100) + float(next_o_put_CMP))
                                        # time.sleep(0.2)
                                        next_sl_trns_id = sl_place_order(smartApi=smartApi, variety = "STOPLOSS", Script = next_put_trade_row['symbol'].iloc[0], token = str(next_put_trade_row['token'].iloc[0]),Status = "BUY", exchange = exch_seg,ordertype = "STOPLOSS_LIMIT", price = str(int(stoploss_price)), quantity = quantity,triggerprice = str(int(stoploss_price)-500),orderid = next_put_trns_id,num_retry = 10, producttype = producttype,broker = broker_name,lotsize = lot,Mofsl = Mofsl)
                                        
                                        order_details = get_order_details_v2(next_sl_trns_id, smartApi,Mofsl,broker_name)
                                        add_row = {"date":date,"Trade_id":f"{next_put_trade_row['symbol'].iloc[0]}_sl_put_1","name":name,"trade_type":"stoploss_buy","token":str(next_put_trade_row['token'].iloc[0]),"time":current_time,"exchange":exch_seg,
"tradingsymbol":next_put_trade_row['symbol'].iloc[0],"quantity":quantity,"strike_price": prev_put_strik+(3*gap),
"market_price":(float(stoploss_price))/100.0,"trigger_price": float(str(int(stoploss_price)-500))/100.0,"num_sl_hits":1,"status":order_details['orderstatus'],"trns_id":next_sl_trns_id,"gap":gap,"expiry_date":expiry_date, "unique_order_id":order_details['uniqueorderid'],"text":order_details['text'],'producttype':producttype}
                                        
                                        trade_df = add_row_to_dataframe(trade_df, add_row)
                                        
                                        update_excel(trade_df,"trade_track.xlsx")
                                    
                                
                                    
    except Exception as e:
        # Handle other exceptions (if any)
        logger.error(f"An error occurred: {e}")
        current_time = datetime.now(kolkata_timezone).strftime('%H:%M')
        time_to_sleep = 60-datetime.now(kolkata_timezone).second
        print( f"Going to sleep for {time_to_sleep} seconds due to error." )
        time.sleep(time_to_sleep)
    
                                                      
        
        