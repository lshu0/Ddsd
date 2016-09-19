import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

def main(date , date_1, date_1_long):
    credentials = GoogleCredentials.get_application_default()
    bigquery_service = build('bigquery', 'v2', credentials=credentials)
    query_request = bigquery_service.jobs()       
    query = """
    select a.shc_item_id, a.shc_item_desc,business_unit, a.prcm_sears_div_nbr, a.sears_line_nbr, a.prcm_sears_itm_nbr, unitcost,
    a.starting_price, b.website_name,  min(b.purchase_price) comp_prc , b.last_crawled_date
    from [shc-pricing-prod:bq_pricing_it.incoming__onlineitems_price_w_hierarchy${to_date}]  a 
    left join [shc-pricing-prod:bq_pricing_it.incoming__market_track_livefeed_3x_refresh${yest_date}] b
    on a.shc_item_id  = b.shc_item_id
    where 
    a.format_id = 'sears'
    and isdispelig = 'true'
    and b.format_name = 'sears'
    and b.stock_status <> 'OOS'
    and b.last_crawled_date between date(date_add('{yest_long}', -3, 'DAY')) and '{yest_long}'
    group by 1,2,3,4,5,6,7,8,9,11
    order by a.shc_item_id;
    """.format(to_date = date, yest_long = date_1_long, yest_date = date_1)     
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
    df =pd.DataFrame(result, columns= ['shc_item_id','shc_item_desc','business_unit','sears_div_nbr','sears_ln_nbr','sears_itm_nbr','unit_cost','starting_price','website_name','comp_prc', 'crawl_date'])
    df.sort_values(by = 'crawl_date', inplace = True)
    df.drop_duplicates(['sears_div_nbr','sears_itm_nbr','website_name'],keep='last',inplace=True)
    pivot = df.set_index(['sears_div_nbr','sears_itm_nbr','website_name'])['comp_prc']
    pivot = pivot.unstack()
    pivot=pivot.convert_objects(convert_numeric=True)
    pivot['min comp price'] = pivot.min(axis = 1)
    prc = df[['shc_item_id','shc_item_desc', 'business_unit','sears_div_nbr','sears_ln_nbr','sears_itm_nbr','unit_cost','starting_price']].drop_duplicates()
    join = pd.merge(prc,pivot, right_index = True , left_on = ['sears_div_nbr','sears_itm_nbr'] , how = 'left')
    join.ix[:, (join.columns != 'shc_item_desc') & (join.columns != 'shc_item_id') & (join.columns != 'business_unit')] = join.ix[:, (join.columns != 'shc_item_desc') & (join.columns != 'shc_item_id') ].convert_objects(convert_numeric=True)   
    import numpy as np
    def division(row):
        if row['starting_price'] == 0 :
            return np.nan
        else:
            return 1 - row['unit_cost']/row['starting_price']
    join['MR_current'] = join.apply(division,axis = 1 )          
    print('table created')
    return join

if __name__ == "__main__":
    import sys
    output = main(sys.argv[1], sys.argv[2], sys.argv[3])
    output.sort_values(by = ['business_unit'], inplace = True)
    output.set_index(keys=['business_unit'], drop = False, inplace = True)
    BUs = output['business_unit'].unique().tolist()
    output_dict = {}
    for BU in BUs:
        output_dict[BU]=output[output['business_unit']==BU] 
    for key in output_dict.keys():
        file_name = 'output'+str(sys.argv[1])+key+'.csv'
        output_dict[key].to_csv(file_name, index = False )

