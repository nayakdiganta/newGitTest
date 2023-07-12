# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 09:50:53 2022

@author: s256351
"""


import os
import pyodbc
import pandas as pd
import numpy as np
import datetime as datetime
import joblib
import math
from scipy import spatial


connection = pyodbc.connect("DSN=AthenaDev;Uid=s256351@CORP.AEPSC.COM;PWD=%s;" % get_pwd(), autocommit=True) 


### Getting the raw data for meter and transformer locations and the current feet distance



# data_dir = '\\\hqceldv05\cties\inbound_gis_data\oh'
# os.chdir(data_dir)
# print(os.listdir(data_dir))

# oh = pd.read_csv('AEPOH_Customers_20221116.csv',
#                  names = ['prem_nb','meter_id','longitude_m','latitude_m','station_nm','circuit_nb','opco' ,'trsf_pole_nb','longitude_t','latitude_t','as_of_dt' ], header = 0)
# oh_samp = oh.head(100)


data_dir = 'H:\\dsmpa\\Shane\\Analytics\\meter_ties\\updated\\'
os.chdir(data_dir)

oh = pd.read_csv('CSPCustomers.csv', names = ['junk', 'prem_nb','trsf_pole_nb','longitude_t','latitude_t','meter_id','latitude_m', 'longitude_m'], header = 0)
oh.dropna(subset = ['trsf_pole_nb', 'longitude_m', 'latitude_m','latitude_t', 'longitude_t'], inplace = True)
oh.drop_duplicates(subset = ['meter_id'],keep = False, inplace = True)

oh['trsf_pole_nb'] = oh['trsf_pole_nb'].astype(str)
oh['meter_id'] = oh['meter_id'].astype(str)
oh = oh[['prem_nb', 'meter_id', 'trsf_pole_nb', 'latitude_m', 'longitude_m', 'latitude_t', 'longitude_t']]

loc2 = oh[(oh['latitude_m']>0) & (oh['latitude_m']<=90)]

# pso = pd.read_csv('PSO_Customers_10192022.csv', sep = '\t',
#                    names = ['prem_nb','meter_id','longitude_m','latitude_m', 'trsf_pole_nb','longitude_t','latitude_t','opco','as_of_dt' ], header = 0)
# pso_samp = pso.head(100)
#loc2 = oh[(oh['latitude_m']>0) & (oh['latitude_m']<=90)]

loc2['meter_cord'] = list(zip(loc2.latitude_m, loc2.longitude_m))
loc2['trans_cord'] = list(zip(loc2.latitude_t, loc2.longitude_t))

from geopy import distance
def distancer(row):
    x = row['meter_cord']
    y = row['trans_cord']
    return distance.distance(x,y).feet
loc2['curr_feet_dist'] = loc2.apply(distancer, axis = 1)





#### Get data from Athena


q1 = '''SELECT distinct  serialnumber as meter_id,
         median_eucl_dist as ed_dist,
         median_z_score_eucl_dist as z_score,
         xf_meter_cnt,
         median_shape_mismatch_cnt as median_shape_mismatch
FROM     stg_cties.meter_xfmr_voltg_summ
WHERE    aep_opco='oh'
AND      aep_usage_dt between '2022-10-01' and '2022-10-15'
AND      xf_meter_cnt > 2
and median_z_score_eucl_dist is not null
'''

df = pd.read_sql(q1,connection)
df.drop_duplicates(inplace = True)



#### Get data from meter prem and only get overhead service
connection2 = pyodbc.connect("DSN=AthenaProd;Uid=s256351@CORP.AEPSC.COM;PWD=%s;" % get_pwd(), autocommit=True) 

q2 = '''select mfr_devc_ser_nbr as meter_id, srvc_entn_cd, serv_city_ad from default.meter_premise where co_cd_ownr = '10' '''
mtr_prem = pd.read_sql(q2, connection2)
keep = ['O','Y']
mtr_prem2 = mtr_prem[mtr_prem['srvc_entn_cd'].isin(keep)]



### filter data and model

loc3 = loc2[loc2['meter_id'].isin(mtr_prem2['meter_id'])]
loc4 = loc3[['meter_id', 'curr_feet_dist']]
df2 = df[df['meter_id'].isin(mtr_prem2['meter_id'])]

df3 = loc4.merge(df2, on = 'meter_id')
df3.drop_duplicates(inplace = True)


final_rf = joblib.load('H:\\dsmpa\\Shane\\Analytics\\meter_ties\\updated\\Rf_ties.joblib')
df3[['pred_0', 'pred_1']] = final_rf.predict_proba(df3[['curr_feet_dist', 'ed_dist', 'z_score', 'median_shape_mismatch']])

def dec(x):
    return int(str(x).split('.')[1][0])
df3['decile'] = df3.apply(lambda row: dec(row['pred_1']), axis = 1)

# For the final model, we may end up going with 7 as opposed to 5
df4 = df3[df3['decile']>= 5]

### Finding the 10 closest transformers for the suspected meters in df4


def cartesian(latitude, longitude, elevation = 0):
    # Convert to radians
    latitude = latitude * (math.pi / 180)
    longitude = longitude * (math.pi / 180)

    R = 6371 # 6378137.0 + elevation  # relative to centre of the earth
    X = R * math.cos(latitude) * math.cos(longitude)
    Y = R * math.cos(latitude) * math.sin(longitude)
    Z = R * math.sin(latitude)
    return (X, Y, Z)


def find_population(lat, lon):
    cartesian_coord = cartesian(lat,lon)
    closest = tree.query([cartesian_coord], p = 2, k = 10)
    i_list = closest[1].tolist()[0]
    c_list = closest[0].tolist()[0]
    c_list2 = [i *3280.84 for i in c_list]
    out = pd.DataFrame(trans.iloc[i_list,:])
    out['dist'] = c_list2
    return out

trans = oh[['trsf_pole_nb','latitude_t', 'longitude_t']]
trans.drop_duplicates(keep='first', inplace = True)




transformer = []
for index, row in trans.iterrows():
    coordinates = [row['latitude_t'], row['longitude_t']]
    cartesian_coord = cartesian(*coordinates)
    transformer.append(cartesian_coord)


    
tree = spatial.KDTree(transformer)

meters2 = oh[oh['meter_id'].isin(df4['meter_id'])]

meters3 = meters2.copy()
meters3.rename(columns={'trsf_pole_nb':'curr_trsf'}, inplace=True)

mt = []
c = 0
for index,row in meters3.iterrows():
    x = row['latitude_m']
    y = row['longitude_m']
    z = find_population(x,y)
    z['meter_id'] = row['meter_id']
    z['curr_trsf'] = row['curr_trsf']
    c+=1
    print(c)
    mt.append(z)
    
new = pd.concat(mt)
new['tran_rank'] = new.groupby('meter_id')['dist'].rank('dense', ascending = True)
new2 = new[['meter_id', 'curr_trsf', 'trsf_pole_nb', 'dist', 'tran_rank']]
new2.drop_duplicates(inplace = True)

#### New2 is dataframe that contains the meters in question and their corresponding 10 closest trans


## Move the list of transformers and meters to AWS to get array data for calculations

t_list = new2['trsf_pole_nb'].unique().tolist()
td = pd.DataFrame(t_list)
td.to_csv('R:/Shane/cties/t_list.csv', index = False, header = False)

t = tuple(new2['trsf_pole_nb'].tolist())

q2 = '''SELECT   Distinct trsf_pole_nb,
         xf_read_array_whole,
         aep_usage_dt
FROM     stg_cties.meter_xfmr_voltg_summ
WHERE    aep_opco='oh'
AND      aep_usage_dt between '2022-10-01' and '2022-10-15'
AND      trsf_pole_nb in (select * from hdpsand.ties_trsf)
ORDER BY trsf_pole_nb, aep_usage_dt
'''

new_data = pd.read_sql(q2,connection)
new_data.drop_duplicates(inplace = True)

# Get data for meters

m = tuple(new2['meter_id'].unique().tolist())

m_list = new2['meter_id'].unique().tolist()
md = pd.DataFrame(m_list)
md.to_csv('R:/Shane/cties/meters/m_list.csv', index = False, header = False)


q3 = '''
SELECT   serialnumber as meter_id,
         trsf_pole_nb,
         read_array,
         aep_usage_dt
FROM     stg_cties.meter_xfmr_voltg_summ
WHERE    aep_opco='oh'
AND       aep_usage_dt between '2022-10-01' and '2022-10-15'
and serialnumber in (select * from hdpsand.ties_meters)
'''

new_meter = pd.read_sql(q3,connection)

# Create functions for calculations on array data


def euclidean_distance(x, y):   
    return np.linalg.norm(x - y)

def clean_calc(x,y):
    x = x.strip('[]').split(',')
    x1 = np.array([float(z) for z in x])
    x2 = np.linalg.norm(x1)
    x3 = x1/x2
    y = y.strip('[]').split(',')
    y1 = np.array([float(z) for z in y])
    y2 = np.linalg.norm(y1)
    y3 = y1/y2
    ed = euclidean_distance(x3,y3)
    return ed

## loop through days and find euclidean distance for each meter and transformer combo

all_d = []
for d in new_data['aep_usage_dt'].unique():
    nd2 = new_data[new_data['aep_usage_dt'] ==d]
    mn2 = new_meter[new_meter['aep_usage_dt']==d]
    nd3 = nd2.merge(new2, on = 'trsf_pole_nb')
    nd4 = nd3.merge(mn2, on = 'meter_id')
    nd5 = nd4[['meter_id','curr_trsf','read_array', 'trsf_pole_nb_x', 'xf_read_array_whole', 'dist', 'tran_rank','aep_usage_dt_x' ]]
    nd5['ed_dist'] = nd5.apply(lambda row: clean_calc(row['read_array'], row['xf_read_array_whole']), axis = 1)
    print(d)
    all_d.append(nd5)

all_d2 = pd.concat(all_d)

all_d2.rename(columns = {'trsf_pole_nb_x': 'trsf_pole_nb'}, inplace = True)

### Finding the transformer with the smallest ED score as the best fit

final2_stats = all_d2.groupby(['meter_id', 'trsf_pole_nb']).agg({'ed_dist':'median'}).reset_index().sort_values(by = ['meter_id', 'ed_dist'])
final2_min = final2_stats.groupby(['meter_id']).agg({'ed_dist':'min'}).reset_index()

final2_min = final2_min.merge(final2_stats, on = ['meter_id', 'ed_dist'])
final2_min = final2_min.merge(new[['meter_id', 'trsf_pole_nb', 'dist', 'tran_rank']], on = ['meter_id', 'trsf_pole_nb'])

final_send = df4.merge(final2_min, on = 'meter_id')
final_send = final_send.rename(columns= { 'trsf_pole_nb': 'new_trsf', 'ed_dist_x':'orig_score','z_score_x':'orig_z_score_meters','ed_dist_y':'new_score','z_score_y':'z_score_xmfr' })


final_send = final_send.merge(meters3[['meter_id','curr_trsf']], on = 'meter_id')
final_send = final_send[['meter_id', 'curr_trsf', 'curr_feet_dist', 'orig_score', 'z_score', 'xf_meter_cnt',
       'median_shape_mismatch', 'pred_0', 'pred_1', 'decile', 'new_score',
       'new_trsf', 'dist', 'tran_rank']]

