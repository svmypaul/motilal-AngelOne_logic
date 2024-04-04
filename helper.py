#-------------------------Normal place order------------------------

# PlaceOrderInfo
Orderinfo = {      
    "exchange":"BSEFO", # NSEFO, BSEFO,BSE,NSE
    "symboltoken":887038, # Exchange Scrip code or Symbol Token is unique identifier
    "buyorsell":"BUY", # BUY, SELL
    "ordertype":"LIMIT", # LIMIT, MARKET, STOPLOSS
    "producttype":"NORMAL", # NORMAL, DELIVERY, SELLFROMDP, VALUEPLUS, BTST,MTF
    "orderduration":"DAY", # DAY,GTC,GTD,IOC
    "price":235, # 
    "triggerprice":0,
    "quantityinlot":1,
    "disclosedquantity":0,
    "amoorder":"N", # Y or N
    "algoid":"", # Algo Id or Blank for Non-Algo Orders
    "tag":" " # Echo back to identify order
}

# Mofsl.PlaceOrder(Orderinfo)
print(Mofsl.PlaceOrder(Orderinfo))

#-----------------------SL place order------------------------------------

# PlaceOrderInfo
Orderinfo = {      
    "uniqueorderid": '2600025ANKN1226',
    "exchange":"NSEFO", # NSEFO, BSEFO,BSE,NSE
    "buyorsell":"SELL", # BUY, SELL
    "ordertype":"STOPLOSS", # LIMIT, MARKET, STOPLOSS
    "orderduration":"DAY", # DAY,GTC,GTD,IOC
    "price":74, # 
    "triggerprice":49,
    "quantityinlot":75,
    "producttype":'NORMAL', # NORMAL, DELIVERY, SELLFROMDP, VALUEPLUS, BTST,MTF
    "amoorder":"N" # Y or N
}

# Mofsl.PlaceOrder(Orderinfo)
print(Mofsl.PlaceOrder(Orderinfo))


d = pd.read_csv('trade_track.csv')
trade_df['status'][3] = 'trigger pending'
