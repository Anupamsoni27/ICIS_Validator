from builtins import print

import pymssql

query_part_1 = """select distinct m.URI from lib_model m where m.category = 'ICIS_ESGM_PSVTS' """
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
data_df = data

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


# How many elements each
# list should have
n = 100

x = list(divide_chunks(data, n))
print(len(x))
for chunk in x :
    line = ''
    for aa in chunk:
        line = line + "<dat:JavaLangstring>" + aa[0] + "</dat:JavaLangstring>"
    print((line))
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
