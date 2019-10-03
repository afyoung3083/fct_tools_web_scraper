import requests
import datetime
now = datetime.datetime.now()
import lxml.html as lh
import pandas as pd
import numpy as np

#lxml only accepts http (not https); so removed the 's'
url = 'http://fct.tools/?data_key=6adf3f70acf1e128380fdbdda9577598'

#create a dataframe of the table
web_data = pd.read_html(url)
df = web_data[0] # the first item in the result is the table we want

#unedited, the columns are as follows:
'''
1)  'Docker'
2)  'Factomd'
3)  'Node name  Location'
4)  'DBlock  Entry'
5)  'Currentmin'
6)  'CPU'
7)  'RAM'
8)  'Diagnostics'
9)  'Factomd image  SHA256'
10) 'Server Identity'
'''

#split factomd up into 'factomd' and 'ldr_status'
df[['factomd','ldr_status']] = df['Factomd'].str.split(expand=True)

#split 'Node name  Location' into 'name', 'ip_addr', and 'port'
df['Node name  Location'] = df['Node name  Location'].str.replace('Â', '')
df[['name','ip_addr','x','port']] =  df['Node name  Location'].str.split(expand=True)

#correct doubled 'DBlock Entry'
df['block_height'] = df['DBlock  Entry'].mod(10**5)

#replace non-ascii character with null space
df['Currentmin'] = df['Currentmin'].str.replace('â', '')

#only consider left most number in 'CPU' column; convert str to int64
df['cores_GB'] = df['CPU'].str[0].astype(np.int64)

#remove last four characters 'Â GB' from 'RAM' column; convert to float
df['ram_GB'] = df['RAM'].str.replace('Â GB', '', regex=True)
df['ram_GB'] = df['ram_GB'].str.replace(' GB', '', regex=True)
df['ram_GB'] = df['ram_GB'].astype(float)

#remove non-ascii characters in 'Diagnostics'; split to leader/audit
df[['ldr_nodes','aud_nodes']] = df['Diagnostics'].str.split('/', expand=True)
df['ldr_nodes'] = df['ldr_nodes'].str[-3:].fillna(value=0).astype(np.int64)
df['aud_nodes'] = df['aud_nodes'].str[-3:].fillna(value=0).astype(np.int64)

#split 'Factomd image  SHA256' into 'version' and 'sha_256'
df['sha_256'] = df['Factomd image  SHA256'].str[-20:]
df['version'] = df['Factomd image  SHA256'].str[:-20]

#Print Testnet report
print("Date/Time (UTC): ", now.utcnow())
print("Number of nodes: ", len(df.index))
print("Block height: ", "{:,}".format(df['block_height'].max()))
print("Information source: ", url[:-42])
print("Total leader nodes: ", len(df[df['ldr_status'] == 'Leader']))
leaders = df[df['ldr_status'].str.contains("Leader")]['name'].values.tolist()
for lead in leaders:
    print("\t", lead)
print("Total audit nodes: ", len(df[df['ldr_status'] == 'Audit']))
audits = df[df['ldr_status'].str.contains("Audit")]['name'].values.tolist()
for aud in audits:
    print("\t", aud)
print("Nodes with current Factomd version: ", len(df[df['version'] == 'v6.4.2-rc2-alpine']))
print("Nodes with old Factomd version: ", len(df[df['version'] != 'v6.4.2-rc2-alpine']))
outdated = df[~df['version'].str.contains('v6.4.2-rc2-alpine')]['name'].values.tolist()
for old in outdated:
    print("\t", old)
print("Nodes with Docker OFF: ", len(df[df['Docker'] == 'OFF']))
dockoff = df[df['Docker'].str.contains('OFF')]['name'].values.tolist()
for d in dockoff:
    print("\t", d)
print("Nodes with factomd OFF: ", len(df[df['factomd'] == 'OFF']))
factomd = df[df['factomd'].str.contains('OFF')]['name'].values.tolist()
for f in factomd:
    print("\t", f)
