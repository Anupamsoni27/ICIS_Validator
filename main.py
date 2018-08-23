import requests
import xml.etree.ElementTree as et
import requests
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import *
import json as js
import xml.etree.ElementTree as et
import pandas as pd
import os
class ICIS_Validator:


    def __init__(self, name):
        self.name = name


    def make_global(self):
     global aaaa


    def model_call(self):
        from builtins import print
        global data

        import pymssql

        query_part_1 = """SELECT m.URI, p.NAME FROM LIB_MODEL m  JOIN LIB_PROFILE p ON m.ID=p.model_id where
m.category = '""" + dataset_name + """' and p.name=m.default_attribute;"""
        query_part_2 = ""
        global dhub_1_list
        global dhub_2_list
        global all_list
        dhub_1_list = []
        dhub_2_list = []
        all_list = []
        query = query_part_1

        svr = "DHUB2"
        server_name = "10.0.9.97"
        user_name = "gdm"
        pswd = "gdm"
        database_name = "GDM"
        conn = pymssql.connect(server=server_name, user=user_name, password=pswd, database=database_name)
        cursor = conn.cursor()

        # script = "select DISTINCT m.category,m.uri from lib_model m where m.code like'%.[0-9][0-9][0-9][0-9]M%' and m.category like'%REL'"
        script = query
        print(script)
        cursor.execute(script)

        data = cursor.fetchall()
        print(data)
        for d in data:
            print(d)

    def data_call(self):
        def divide_chunks(l, n):
            print("running data_call")

            # looping till length l
            for i in range(0, len(l), n):
                yield l[i:i + n]

        # How many elements each
        # list should have
        n = 100

        chunks = list(divide_chunks(data, n))
        final_df1 = pd.DataFrame()
        for chunk in chunks:
            line = ""
            for aa in chunk:
                line = line +  "<dat:JavaLangstring>" + aa[0] + "/" + aa[1] +"/ALL</dat:JavaLangstring>"
            print(line)
            final_df = pd.DataFrame()

            # datetimeObject = datetime.strptime(start_date,"%d-%m-%Y")
            # new_start_date =datetimeObject.strftime("%Y-%m-%d")
            # datetimeObject = datetime.strptime(end_date,"%d-%m-%Y")
            # new_end_date =datetimeObject.strftime("%Y-%m-%d")
            # print(new_start_date,new_end_date)
            # a=pd.DatetimeIndex(start=new_start_date,end=new_end_date, freq=BDay())

            url = "http://10.0.9.95:80/gdm/DataActionsService"
            headers = {'content-type': 'application/soap+xml'}
            # headers = {'content-type': 'text/xml'}
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

            body3 = """</dat:uris>
                     <dat:rangeUri>range://default/default</dat:rangeUri>
                  </dat:getGenicDatas>
               </soapenv:Body>
            </soapenv:Envelope>
            """
            body2 = line
            body = body1 + body2 + body3
            response = requests.post(url, data=body,
                                     headers=headers)  # ,username="adminuser@domain",password="adminuser@domain",)
            r = response.text
            # print(r)

            with open('GDMResponse.xml', 'w')as f:
                f.write(r)
            tree = et.ElementTree(et.fromstring(response.text))
            root = tree.getroot()
            for element in root:
                if element.tag == "{http://schemas.xmlsoap.org/soap/envelope/}Body":
                    for ns4 in element:
                        if ns4.tag == "{http://www.datagenicgroup.com}getGenicDatasResponse":
                            for ns4_temp in ns4:
                                if ns4_temp.tag == "{http://www.datagenicgroup.com}return":
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
                                                                    if property.tag == "{java:com.datagenicgroup.data}Value":
                                                                        # print(property.text)
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
                                                                        print(ModelUri)
                                                        if properties.tag == "{java:com.datagenicgroup.data}Values":
                                                            temp_model_data.append(properties.text)
                                                            # print(properties.text)

                                            if len(temp_model_data) > 0:
                                                start_date = FullRangeUri.split("/")[-2]
                                                end_date = FullRangeUri.split("/")[-1]
                                                datetimeObject = datetime.strptime(start_date, "%d-%m-%Y")
                                                new_start_date = datetimeObject.strftime("%Y-%m-%d")
                                                datetimeObject = datetime.strptime(end_date, "%d-%m-%Y")
                                                new_end_date = datetimeObject.strftime("%Y-%m-%d")
                                                if "DB:1" in FullRangeUri :
                                                    a = pd.DatetimeIndex(start=new_start_date, end=new_end_date, freq=BDay())
                                                elif "D:1" in FullRangeUri:
                                                    a = pd.DatetimeIndex(start=new_start_date, end=new_end_date, freq=Day())


                                                temp_datelist = []
                                                for temp_date in a:
                                                    temp_datelist.append(temp_date)

                                            print(FullRangeUri)
                                            print(len(temp_datelist))
                                            print(len(temp_model_data))
                                            print("######################33")
                                            df = pd.DataFrame()
                                            #
                                            # print(len(temp_datelist))
                                            # print(len(temp_model_data))
                                            temp_datelist = temp_datelist
                                            temp_model_data = temp_model_data
                                            df["Date"] = temp_datelist
                                            df["value"] = temp_model_data
                                            df["Model Code"] = ModelUri
                                            df.sort_values('Date', ascending=False)
                                            # df.append(pd.DataFrame(temp_datelist))
                                            # df.append(pd.DataFrame(temp_model_data))
                                            # print(df)
                                            final_df = final_df.append(df)
                                            print(final_df.head(10))
                                            print("###################################################################################################################################")

            final_df1 = final_df1.append(final_df)
        final_df1.to_csv("files_to_validate/model_data.csv", index=False)

    def read_input(self):
        list1 = []
        for root, dir, files in os.walk("inputfiles/"):
            for file in files:
                tree = et.parse("inputfiles/" + file)

                date = ""
                period = ""
                volume = ""
                total = ""
                price = ""
                root = tree.getroot()
                for Transactions in root:
                    for Transaction in Transactions:
                        for record in Transaction:
                            for COLUMN in record:
                                if COLUMN.attrib["FormalName"] == "date":
                                    date = COLUMN.text
                                if COLUMN.attrib["FormalName"] == "period":
                                    period = COLUMN.text
                                if COLUMN.attrib["FormalName"] == "volume":
                                    volume = COLUMN.text
                                if COLUMN.attrib["FormalName"] == "total":
                                    total = COLUMN.text
                                if COLUMN.attrib["FormalName"] == "price":
                                    price = COLUMN.text
                            temp_row = date, period, volume, total, price
                            list1.append(temp_row)
        for l in list1:
            print(l)
        list1 = list(set(list1))
        temp_df = pd.DataFrame(list1, columns=["Date", "Period", "Volume", "total", "price"])
        temp_df.to_csv("files_to_validate/file.csv", index=False)


dbObj = ICIS_Validator("Connect MS SQL")

dataset_list = ['TRAF_MONEY']
for dataset_name in dataset_list:

    dbObj.make_global()
    dbObj.model_call()
    dbObj.data_call()
    dbObj.read_input()