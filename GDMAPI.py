# encoding=utf8
import requests
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import *
import json as js
from requests.auth import HTTPBasicAuth

URL = 'http://dgints04:83/gdm-rs/data/genicdataasjson?uri=model%3A%2F%2FECB_FX%2FEU.FX.SPOT.ECB.CET.TRY.EUR&rangeUri=range%3A%2F%2Fdefault%2Fdefault'
username = 'adminuser@domain'
password = 'adminuser@domain'
r = requests.get(URL, auth=HTTPBasicAuth(username, password))
data = js.loads(r.text)
i_json_data = js.load(data)
for i in i_json_data['NumericSeries']['Properties']:
     if i['Name']== 'ModelDescription':
      model_description = (i['Value'])
     elif i['Name']== 'ModelUri':
      model_uri = i['Value']
     elif i['Name']== "Resource":
      category =  i['Value']

range  = i_json_data['NumericSeries']['RangeURI']
values = i_json_data['NumericSeries']['Values']
print(model_uri)
print(model_description)
print(category)
print(range)
start_date = range.split('/')[4]
end_date_info = range.split('/')[5]
end_date = end_date_info[:10]
datetimeObject = datetime.strptime(start_date,"%d-%m-%Y")
new_start_date =datetimeObject.strftime("%Y-%m-%d")
datetimeObject = datetime.strptime(end_date,"%d-%m-%Y")
new_end_date =datetimeObject.strftime("%Y-%m-%d")
print(new_start_date,new_end_date)
a=pd.DatetimeIndex(start=new_start_date,end=new_end_date, freq=BDay())
date_list =[]
print(type(values))
for i in a:
    date_list.append(i)
print(len(date_list))
print(len(values))
df = pd.DataFrame()
df["Date"] =date_list
df["Values"] = values
df.append(pd.DataFrame(date_list))
df.append(pd.DataFrame(values))
print(df)
df.to_csv("model_data.csv",index=False)




