import requests
import xml.etree.ElementTree as et
import requests
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import *
import json as js
from requests.auth import HTTPBasicAuth
from xml.etree import ElementTree
final_df = pd.DataFrame()
#
# atetimeObject = datetime.strptime(start_date,"%d-%m-%Y")
# new_start_date =datetimeObject.strftime("%Y-%m-%d")
# datetimeObject = datetime.strptime(end_date,"%d-%m-%Y")
# new_end_date =datetimeObject.strftime("%Y-%m-%d")
# print(new_start_date,new_end_date)
# a=pd.DatetimeIndex(start=new_start_date,end=new_end_date, freq=BDay())



url="http://10.0.9.61:80/gdm/DataActionsService"
headers = {'content-type': 'application/soap+xml'}
#headers = {'content-type': 'text/xml'}
body1 = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:dat="http://www.datagenicgroup.com">
<soapenv:Header>
      <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
         <wsse:UsernameToken wsu:Id="UsernameToken-902788241" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
            <wsse:Username>adminuser@domain</wsse:Username>
            <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">adminuser@domain</wsse:Password>
         </wsse:UsernameToken>
      </wsse:Security>
      <ns1:Client soapenv:actor="http://schemas.xmlsoap.org/soap/actor/next" soapenv:mustUnderstand="0" xmlns:ns1="gdm:http://www.datagenicgroup.com">
         <ns1:ApplicationType>JavaClient</ns1:ApplicationType>
      </ns1:Client>
   </soapenv:Header>
   <soapenv:Body>
      <dat:getGenicDatas>
         <dat:uris>"""
body2 ="""<dat:JavaLangstring>model://CME_NYMEX_8K/US.NYMEX.CME.REGULAR.FUT.8K.2018M01/SETTLE/ALL</dat:JavaLangstring>"""
body3 = """</dat:uris>
                 <dat:rangeUri>range://default/default</dat:rangeUri>
              </dat:getGenicDatas>
            </soapenv:Body>
            </soapenv:Envelope>
            """
body = body1+body2+body3
response = requests.post(url,data=body,headers=headers)#,username="adminuser@domain",password="adminuser@domain",)
r = response.text
print (r)

with open('GDMResponse.xml', 'w')as f:
    f.write(r)
tree = et.ElementTree(et.fromstring(response.text))
root = tree.getroot()
for element in root:
    if element.tag == "{http://schemas.xmlsoap.org/soap/envelope/}Body":
     for ns4 in element:
         if ns4.tag == "{http://www.datagenicgroup.com}getGenicDatasResponse":
             for ns4_temp in ns4:
                 if ns4_temp.tag =="{http://www.datagenicgroup.com}return":
                     for ns4_temp_2 in ns4_temp:
                         if ns4_temp_2.tag == "{http://www.datagenicgroup.com}GenicData":
                             FullRangeUri = ""
                             temp_model_data = []
                             for genicData in ns4_temp_2:

                                 if genicData.tag == "{java:com.datagenicgroup.data}NumericSeries":
                                     for properties in genicData:

                                         for property in properties:
                                             # print(property.text)
                                             if property.text == "FullRangeUri":
                                                 for property in properties:
                                                     # print(property.tag)
                                                     if property.tag  == "{java:com.datagenicgroup.data}Value":
                                                         print(property.text)
                                                         FullRangeUri = property.text
                                             if property.text == "ModelName":
                                                 for property in properties:
                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                        ModelName = property.text
                                             if property.text == "ModelDescription":
                                                 for property in properties:
                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                        ModelDescription = property.text
                                             if property.text == "ModelUri":
                                                 for property in properties:
                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                        ModelUri = property.text

                                         if properties.tag == "{java:com.datagenicgroup.data}Values":
                                          temp_model_data.append(properties.text)
                                          print(properties.text)
                             if len(temp_model_data) > 0:
                                 start_date = FullRangeUri.split("/")[-2]
                                 end_date = FullRangeUri.split("/")[-1]
                                 datetimeObject = datetime.strptime(start_date, "%d-%m-%Y")
                                 new_start_date = datetimeObject.strftime("%Y-%m-%d")
                                 datetimeObject = datetime.strptime(end_date, "%d-%m-%Y")
                                 new_end_date = datetimeObject.strftime("%Y-%m-%d")
                                 a = pd.DatetimeIndex(start=new_start_date, end=new_end_date, freq=BDay())
                                 temp_datelist = []
                                 for temp_date in a:
                                     temp_datelist.append(temp_date)

                             for t in temp_datelist:
                                 print(t)
                             print(len(temp_datelist))
                             print(len(temp_model_data))
                             df = pd.DataFrame()

                             print(len(temp_datelist))
                             print(len(temp_model_data))
                             temp_datelist = temp_datelist
                             temp_model_data =  temp_model_data
                             df["Date"] = temp_datelist
                             df["value"] = temp_model_data
                             df["Model Code"] = ModelUri
                             df.sort_values('Date',ascending=False)
                             # df.append(pd.DataFrame(temp_datelist))
                             # df.append(pd.DataFrame(temp_model_data))
                             # print(df)
                             final_df = final_df.append(df)
final_df.to_csv("model_data231.csv", index=False)
