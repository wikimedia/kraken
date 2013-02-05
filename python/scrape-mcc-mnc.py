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
rows = []
for row_soup in tb.findAll('tr'):
    rows.append([c.renderContents() for c in row_soup.findAll('td')])
    
mcc_df = pd.DataFrame(rows, columns=headers)
mcc_df['key'] = mcc_df['MCC'].map(str) + '-' + mcc_df['MNC'].map(str)

def clean(s):
    return s.strip().lower().replace('.','').replace(' ','-')

mcc_df['str'] = mcc_df['Network'].dropna().map(clean) + '-' + mcc_df['Country'].dropna().map(clean)

mcc_df = mcc_df.drop(0)
mcc_df.to_csv('mcc_mnc.csv', index=False)

d_full = [r[1].to_dict() for r in mcc_df.iterrows()]
json.dump(d_full, open('mcc_mnc.json', 'w'), indent=2)

d_min = dict(zip(mcc_df['key'],mcc_df['str']))
json.dump(d_min,  open('mcc_mnc.min.json', 'w'), indent=2)
