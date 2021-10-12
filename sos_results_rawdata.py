import pandas as pd
import numpy as np
import requests
import os
import zipfile

# Read the file from website
url='https://miboecfr.nictusa.com/cfr/presults/'
cycle='2020'
file= cycle + 'GEN.zip'
r = requests.get(url + file,  allow_redirects=True)
open('raw_data/' + file, 'wb').write(r.content)

archive = zipfile.ZipFile('raw_data/' + file)

# Build some keys for column names from the ReadMe
names_columns = ['cycle',
  'election_type',
  'office_code',
  'district_code',
  'status_code',
  'candidate_id',
  'candidate_last_name',
  'candidate_first_name',
  'candidate_middle_name',
  'candidate_party'
  ]

offices_columns = [
  'cycle',
  'election_type',
  'office_code',
  'district_code',
  'status_code',
  'office_description',
  ]

votes_columns = [
  'cycle',
  'election_type',
  'office_code',
  'district_code',
  'status_code',
  'candidate_id',
  'county_code',
  'juris_code',
  'ward_number',
  'precinct_number',
  'precinct_label',
  'precinct_votes'
  ]

cities_columns = [
  'cycle',
  'election_type',
  'county_code',
  'juris_code',
  'juris_name'
  ]

counties_columns = [
  'county_code',
  'county_name'
  ]

# Load data from each dictionairy into dataframes
df_counties = pd.read_csv(archive.open('county.txt'),delimiter='\t',names=counties_columns,index_col=False)
df_cities = pd.read_csv(archive.open(cycle + 'city.txt'),delimiter='\t',names=cities_columns,index_col=False)
df_names = pd.read_csv(archive.open(cycle + 'name.txt'),delimiter='\t',names=names_columns,index_col=False)
df_offices = pd.read_csv(archive.open(cycle + 'offc.txt'),delimiter='\t',names=offices_columns,index_col=False)
df_votes = pd.read_csv(archive.open(cycle + 'vote.txt'),delimiter='\t',names=votes_columns,index_col=False)

# Build Combined DataFrames
df_results = df_votes.merge(
        df_counties,on='county_code').merge(
            df_cities[['county_code','juris_code','juris_name']],on=['county_code','juris_code']).merge(
                df_offices[['office_code','district_code','status_code','office_description']], on=['office_code','district_code','status_code']).merge(
                    df_names[['office_code','district_code','status_code','candidate_id','candidate_last_name','candidate_first_name','candidate_middle_name','candidate_party']],on=['office_code','district_code','status_code','candidate_id'])

# Drop pollbook totals lines
df_results=df_results[df_results['office_description']!='POLL BOOK TOTALS (TOTAL VOTERS)']

# Add Unified Office Names
def office_names(row):
    if row['office_code']==1:
        return 'President of the United States'
    elif row['office_code']==2:
        return 'Governor'
    elif row['office_code']==3:
        return 'Secretary of State'
    elif row['office_code']==4:
        return 'Attorney General'
    elif row['office_code']==5:
        return 'United States Senator'
    elif row['office_code']==6:
        return 'U.S. Representative in Congress'
    elif row['office_code']==7:
        return 'State Senator'
    elif row['office_code']==8:
        return 'State Representative'
    elif row['office_code']==9:
        return 'Member of the State Board of Education'
    elif row['office_code']==10:
        return 'Member of the University of Michigan Board of Regents'
    elif row['office_code']==11:
        return 'Member of the Michigan State University Board of Trustees'
    elif row['office_code']==12:
        return 'Member of the Wayne State University Board of Governors'
    elif row['office_code']==13:
        return 'Justice of the Supreme Court'
    elif row['office_code']==90:
        return 'Statewide Ballot Proposals'

df_results['office_name']=df_results.apply(office_names, axis=1)

# Long Party Name
def party_names(row):
    if row['candidate_party']=='DEM':
        return 'Democratic'
    elif row['candidate_party']=='GRN':
        return 'Green'
    elif row['candidate_party']=='LIB':
        return 'Libertarian'
    elif row['candidate_party']=='NLP':
        return 'Natural Law'
    elif row['candidate_party']=='NPA':
        return 'No Party Affiliation'
    elif row['candidate_party']=='REP':
        return 'Republican'
    elif row['candidate_party']=='RFP':
        return 'Reform'
    elif row['candidate_party']=='TIS':
        return 'TIS'
    elif row['candidate_party']=='UST':
        return 'US Taxpayers'
    elif row['candidate_party']=='WORW':
        return 'Workers World'

df_results['party_name']=df_results.apply(party_names, axis=1)

# Get Status from Status Code and ReadMe
def statuses(row):
    if row['status_code']==0:
        return 'Regular Term'
    elif row['status_code']==1:
        return 'Non-Incumbent'
    elif (row['status_code']>=2) & (row['status_code']<=4):
        return 'Incumbent - Partial Term'
    elif (row['status_code']>=5) & (row['status_code']<=7):
        return 'Non-Incumbent - Partial Term'
    elif row['status_code']==8:
        return 'Partial Term'
    elif (row['status_code']>=9) & (row['status_code']<=10):
        return 'New Judgeship'

df_results['status_name']=df_results.apply(statuses, axis=1)

# Create Binary for Partial Terms from Status. Drop these for vote aggregates
def is_partial(row):
    if (row['status_code']>=2) & (row['status_code']<=8):
        return True
    else:
        return False

df_results['is_partial']=df_results.apply(is_partial, axis=1)

# Output Combined Result Files to csv

df_results.to_csv('processed_data/' + cycle + '_combined.csv', index=False)

# Quick Summary of Results
print('*****************************')
print('******* QUICK SUMMARY *******')
print('*****************************\n')

print('Total Votes By Office\n' + str(df_results['precinct_votes'].groupby(df_results['office_name']).sum()))

print('\n ----- \n')

print('Presidential Votes by Party: \n' + str(df_results[df_results['office_name']=='President of the United States']['precinct_votes'].groupby(df_results['party_name']).sum()))
