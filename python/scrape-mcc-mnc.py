# coding: utf-8
from BeautifulSoup import BeautifulSoup
import requests
import pandas as pd
import json
import re

p = requests.get('http://www.mcc-mnc.com/')
soup = BeautifulSoup(p.content)
tb = soup.findAll('table', attrs={'id' : 'mncmccTable'})[0]
soup = BeautifulSoup(p.content)
tb = soup.findAll('table', attrs={'id' : 'mncmccTable'})[0]
headers = [h.renderContents() for h in tb.findAll('th')]
headers = [h.title().replace(' ','') if h != h.upper() else h for h in headers ]
rows = []
for row_soup in tb.findAll('tr'):
    rows.append([c.renderContents() for c in row_soup.findAll('td')])

# construct df and drop empty row
mcc_df = pd.DataFrame(rows, columns=headers)
mcc_df = mcc_df.drop(0)

# add custom fields
mcc_df['MNC'] = ['0%s' % mnc if len(mnc) == 1 else '%s' % mnc for mnc in mcc_df['MNC']]
mcc_df['MCC_MNC'] = mcc_df['MCC'].map(str) + '-' + mcc_df['MNC'].map(str)

def clean(s):
    return s.strip().lower().replace('.','').replace(' ','-').replace(',','')

mcc_df['Name'] = mcc_df['Network'].dropna().map(clean) + '-' + mcc_df['Country'].dropna().map(clean)

mcc_df['ISO'] = mcc_df['ISO'].apply(str.upper)


# export

mcc_df.to_csv('mcc_mnc.csv', index=False)

d_full = [r[1].to_dict() for r in mcc_df.iterrows()]
json.dump(d_full, open('mcc_mnc.json', 'w'), indent=2)

d_min = dict(zip(mcc_df['MCC_MNC'],mcc_df['Name']))
json.dump(d_min,  open('mcc_mnc.min.json', 'w'), indent=2)
