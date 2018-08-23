import xml.etree.ElementTree as et
import pandas as pd
import os
list1 = []
for root, dir, files in os.walk("inputfiles/"):
    for file in files:
        tree = et.parse("inputfiles/"+file)

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
                        if COLUMN.attrib["FormalName"] ==  "period":
                            period = COLUMN.text
                        if COLUMN.attrib["FormalName"] =="volume":
                            volume = COLUMN.text
                        if COLUMN.attrib["FormalName"] == "total":
                            total = COLUMN.text
                        if COLUMN.attrib["FormalName"] == "price":
                            price = COLUMN.text
                    temp_row = date,period,volume,total,price
                    list1.append(temp_row)
for l in list1:
    print(l)
list1 = list(set(list1))
temp_df = pd.DataFrame(list1, columns=["Date","Period","Volume","total","price"])
temp_df.to_csv("input_csv_files/file.csv",index=False)
