# coding: utf-8
from BeautifulSoup import BeautifulSoup
import requests
import pandas as pd
import json
import re

p = requests.get('http://enterpriseios.com/wiki/Complete_List_of_iOS_User_Agent_Strings')
soup = BeautifulSoup(p.content)
tables = soup.findAll('table', attrs={'class' : 'mediawiki_table  xsmall'})[1:]
soup = BeautifulSoup(p.content)

headers = ['UserAgentPrefix', 'AppleProduct', 'IOSVersion', 'Build', 'Introduced']

rows = []
for tb in tables:
    for row_soup in tb.findAll('tr')[1:]:
        rows.append([c.renderContents() for c in row_soup.findAll('td')])

# construct df and drop empty row
ios_df = pd.DataFrame(rows, columns=headers)
ios_df = ios_df.drop(0)

def clean(s):
    return s.strip().replace(',', ';').replace('"', '')

ios_df['UserAgentPrefix'] = ios_df['UserAgentPrefix'].map(clean)
ios_df['AppleProduct'] = ios_df['AppleProduct'].map(clean)
ios_df['IOSVersion'] = ios_df['IOSVersion'].map(clean)
ios_df['Build'] = ios_df['Build'].map(clean)
ios_df['Introduced'] = ios_df['Introduced'].map(clean)
ios_df['key'] = ['%s' % product.split(' ')[0] for product in ios_df['AppleProduct']]
ios_df['Product'] = ios_df['key'].map(str) + '-' + ios_df['Build'].map(str)
del ios_df['key']

# export

ios_df.to_csv('ios.csv', index=False)

d_full = [r[1].to_dict() for r in ios_df.iterrows()]
json.dump(d_full, open('ios.json', 'w'), indent=2)
