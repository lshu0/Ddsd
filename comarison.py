import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
bigquery_service = build('bigquery', 'v2', credentials=credentials)
query_request = bigquery_service.jobs()



query = """
select a.shc_item_id, a.shc_item_desc, a.prcm_sears_div_nbr, a.sears_line_nbr, a.prcm_sears_itm_nbr, unitcost,
a.starting_price, b.website_name, b.purchase_price, b.last_crawled_date
from [shc-pricing-prod:bq_pricing_it.incoming__onlineitems_price_w_hierarchy$20160915]  a 
left join [shc-pricing-prod:bq_pricing_it.incoming__market_track_livefeed_3x_refresh$20160914] b
on a.shc_item_id  = b.shc_item_id
where 
a.format_id = 'sears'
and isdispelig = 'true'
and b.format_name = 'sears'
and b.last_crawled_date between date(date_add('2016-09-14', -3, 'DAY')) and '2016-09-14'
group by 1,2,3,4,5,6,7,8,9,10
order by a.shc_item_id;
"""

query_request_body = {'query': (query)}
result = []
query_response = query_request.query(projectId='shc-pricing-dev',body=query_request_body).execute()
while True:
    for row in query_response['rows']:
        result.append([d['v'] for d in row['f']])
    job_ref = query_response['jobReference']
    if 'pageToken' not in query_response.keys():
        break
    token = query_response['pageToken']
    query_response = query_request.getQueryResults(projectId=job_ref['projectId'],jobId=job_ref['jobId'],pageToken=token).execute()

print('query finished')

import pandas as pd
df =pd.DataFrame(result, columns= ['shc_item_id','shc_item_desc','sears_div_nbr','sears_ln_nbr','sears_itm_nbr','unit_cost','starting_price','website_name','comp_prc', 'crawl_date'])
df.sort('crawl_date', inplace = True)
df.drop_duplicates(['sears_div_nbr','sears_itm_nbr','website_name'],keep='last',inplace=True)
pivot = df.set_index(['sears_div_nbr','sears_itm_nbr','website_name'])['comp_prc']
pivot = pivot.unstack()
pivot=pivot.convert_objects(convert_numeric=True)
pivot['min comp price'] = pivot.min(axis = 1)
prc = df[['shc_item_id','shc_item_desc','sears_div_nbr','sears_ln_nbr','sears_itm_nbr','unit_cost','starting_price']].drop_duplicates()
join = pd.merge(prc,pivot, right_index = True , left_on = ['sears_div_nbr','sears_itm_nbr'] , how = 'left')
join.ix[:, (join.columns != 'shc_item_desc') & (join.columns != 'shc_item_id') ] = join.ix[:, (join.columns != 'shc_item_desc') & (join.columns != 'shc_item_id') ].convert_objects(convert_numeric=True)

import numpy as np
def division(row):
    if row['starting_price'] == 0 :
        return np.nan
    else:
        return 1 - row['unit_cost']/row['starting_price']
join['MR_current'] = join.apply(division,axis = 1 )          

join.to_csv('output.csv', index = False )
