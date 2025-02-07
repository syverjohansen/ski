import pandas as pd
import polars as pl
import time
start_time = time.time()


'''m_all = pd.read_excel('~/ski/elo/python/ski/excel365/M.xlsx')#, infer_schema_length=10000)
m_distance = pd.read_excel('~/ski/elo/python/ski/excel365/M_Distance.xlsx')
m_distance_c = pd.read_excel('~/ski/elo/python/ski/excel365/M_Distance_C.xlsx')
m_distance_f = pd.read_excel('~/ski/elo/python/ski/excel365/M_Distance_F.xlsx')
m_sprint = pd.read_excel('~/ski/elo/python/ski/excel365/M_Sprint.xlsx')
m_sprint_c = pd.read_excel('~/ski/elo/python/ski/excel365/M_Sprint_C.xlsx')
m_sprint_f = pd.read_excel('~/ski/elo/python/ski/excel365/M_Sprint_F.xlsx')
m_c = pd.read_excel('~/ski/elo/python/ski/excel365/M_C.xlsx')
m_f = pd.read_excel('~/ski/elo/python/ski/excel365/M_F.xlsx')

l_all = pd.read_excel('~/ski/elo/python/ski/excel365/L.xlsx')
l_distance = pd.read_excel('~/ski/elo/python/ski/excel365/L_Distance.xlsx')
l_distance_c = pd.read_excel('~/ski/elo/python/ski/excel365/L_Distance_C.xlsx')
l_distance_f = pd.read_excel('~/ski/elo/python/ski/excel365/L_Distance_F.xlsx')
l_sprint = pd.read_excel('~/ski/elo/python/ski/excel365/L_Sprint.xlsx')
l_sprint_c = pd.read_excel('~/ski/elo/python/ski/excel365/L_Sprint_C.xlsx')
l_sprint_f = pd.read_excel('~/ski/elo/python/ski/excel365/L_Sprint_F.xlsx')
l_c = pd.read_excel('~/ski/elo/python/ski/excel365/L_C.xlsx')
l_f = pd.read_excel('~/ski/elo/python/ski/excel365/L_F.xlsx')'''

split1 = time.time() - start_time
print(split1)

##Testing excel -- S1, S2, S3, Total-S1 = 229.163, 537.022, 343.764, 880.786

'''m_all.to_excel('~/ski/elo/python/ski/excel365/M.xlsx')
m_distance.to_excel('~/ski/elo/python/ski/excel365/M_Distance.xlsx')
m_distance_c.to_excel('~/ski/elo/python/ski/excel365/M_Distance_C.xlsx')
m_distance_f.to_excel('~/ski/elo/python/ski/excel365/M_Distance_F.xlsx')
m_sprint.to_excel('~/ski/elo/python/ski/excel365/M_Sprint.xlsx')
m_sprint_c.to_excel('~/ski/elo/python/ski/excel365/M_Sprint_C.xlsx')
m_sprint_f.to_excel('~/ski/elo/python/ski/excel365/M_Sprint_F.xlsx')
m_c.to_excel('~/ski/elo/python/ski/excel365/M_C.xlsx')
m_f.to_excel('~/ski/elo/python/ski/excel365/M_F.xlsx')

l_all.to_excel('~/ski/elo/python/ski/excel365/L.xlsx')
l_distance.to_excel('~/ski/elo/python/ski/excel365/L_Distance.xlsx')
l_distance_c.to_excel('~/ski/elo/python/ski/excel365/L_Distance_C.xlsx')
l_distance_f.to_excel('~/ski/elo/python/ski/excel365/L_Distance_F.xlsx')
l_sprint.to_excel('~/ski/elo/python/ski/excel365/L_Sprint.xlsx')
l_sprint_c.to_excel('~/ski/elo/python/ski/excel365/L_Sprint_C.xlsx')
l_sprint_f.to_excel('~/ski/elo/python/ski/excel365/L_Sprint_F.xlsx')
l_c.to_excel('~/ski/elo/python/ski/excel365/L_C.xlsx')
l_f.to_excel('~/ski/elo/python/ski/excel365/L_F.xlsx')
split2 = time.time() - start_time-split1
print(split2)

m_all = pd.read_excel('~/ski/elo/python/ski/excel365/M.xlsx')
m_distance = pd.read_excel('~/ski/elo/python/ski/excel365/M_Distance.xlsx')
m_distance_c = pd.read_excel('~/ski/elo/python/ski/excel365/M_Distance_C.xlsx')
m_distance_f = pd.read_excel('~/ski/elo/python/ski/excel365/M_Distance_F.xlsx')
m_sprint = pd.read_excel('~/ski/elo/python/ski/excel365/M_Sprint.xlsx')
m_sprint_c = pd.read_excel('~/ski/elo/python/ski/excel365/M_Sprint_C.xlsx')
m_sprint_f = pd.read_excel('~/ski/elo/python/ski/excel365/M_Sprint_F.xlsx')
m_c = pd.read_excel('~/ski/elo/python/ski/excel365/M_C.xlsx')
m_f = pd.read_excel('~/ski/elo/python/ski/excel365/M_F.xlsx')


l_all = pd.read_excel('~/ski/elo/python/ski/excel365/L.xlsx')
l_distance = pd.read_excel('~/ski/elo/python/ski/excel365/L_Distance.xlsx')
l_distance_c = pd.read_excel('~/ski/elo/python/ski/excel365/L_Distance_C.xlsx')
l_distance_f = pd.read_excel('~/ski/elo/python/ski/excel365/L_Distance_F.xlsx')
l_sprint = pd.read_excel('~/ski/elo/python/ski/excel365/L_Sprint.xlsx')
l_sprint_c = pd.read_excel('~/ski/elo/python/ski/excel365/L_Sprint_C.xlsx')
l_sprint_f = pd.read_excel('~/ski/elo/python/ski/excel365/L_Sprint_F.xlsx')
l_c = pd.read_excel('~/ski/elo/python/ski/excel365/L_C.xlsx')
l_f = pd.read_excel('~/ski/elo/python/ski/excel365/L_F.xlsx')'''


#parquet -- 343.772, 9.7, 2.270, 11.97

'''m_all = m_all.astype(str)
m_distance = m_distance.astype(str)
m_distance_c = m_distance_c.astype(str)
m_distance_f = m_distance_f.astype(str)
m_sprint = m_sprint.astype(str)
m_sprint_c = m_sprint_c.astype(str)
m_sprint_f = m_sprint_f.astype(str)
m_c = m_c.astype(str)
m_f = m_f.astype(str)

l_all = l_all.astype(str)
l_distance = l_distance.astype(str)
l_distance_c = l_distance_c.astype(str)
l_distance_f = l_distance_f.astype(str)
l_sprint = l_sprint.astype(str)
l_sprint_c = l_sprint_c.astype(str)
l_sprint_f = l_sprint_f.astype(str)
l_c = l_c.astype(str)
l_f = l_f.astype(str)

m_all.to_parquet('~/ski/elo/python/ski/excel365/M.parquet')
m_distance.to_parquet('~/ski/elo/python/ski/excel365/M_Distance.parquet')
m_distance_c.to_parquet('~/ski/elo/python/ski/excel365/M_Distance_C.parquet')
m_distance_f.to_parquet('~/ski/elo/python/ski/excel365/M_Distance_F.parquet')
m_sprint.to_parquet('~/ski/elo/python/ski/excel365/M_Sprint.parquet')
m_sprint_c.to_parquet('~/ski/elo/python/ski/excel365/M_Sprint_C.parquet')
m_sprint_f.to_parquet('~/ski/elo/python/ski/excel365/M_Sprint_F.parquet')
m_c.to_parquet('~/ski/elo/python/ski/excel365/M_C.parquet')
m_f.to_parquet('~/ski/elo/python/ski/excel365/M_F.parquet')

l_all.to_parquet('~/ski/elo/python/ski/excel365/L.parquet')
l_distance.to_parquet('~/ski/elo/python/ski/excel365/L_Distance.parquet')
l_distance_c.to_parquet('~/ski/elo/python/ski/excel365/L_Distance_C.parquet')
l_distance_f.to_parquet('~/ski/elo/python/ski/excel365/L_Distance_F.parquet')
l_sprint.to_parquet('~/ski/elo/python/ski/excel365/L_Sprint.parquet')
l_sprint_c.to_parquet('~/ski/elo/python/ski/excel365/L_Sprint_C.parquet')
l_sprint_f.to_parquet('~/ski/elo/python/ski/excel365/L_Sprint_F.parquet')
l_c.to_parquet('~/ski/elo/python/ski/excel365/L_C.parquet')
l_f.to_parquet('~/ski/elo/python/ski/excel365/L_F.parquet')
split2 = time.time() - start_time-split1
print(split2)

m_all = pd.read_parquet('~/ski/elo/python/ski/excel365/M.parquet')
m_distance = pd.read_parquet('~/ski/elo/python/ski/excel365/M_Distance.parquet')
m_distance_c = pd.read_parquet('~/ski/elo/python/ski/excel365/M_Distance_C.parquet')
m_distance_f = pd.read_parquet('~/ski/elo/python/ski/excel365/M_Distance_F.parquet')
m_sprint = pd.read_parquet('~/ski/elo/python/ski/excel365/M_Sprint.parquet')
m_sprint_c = pd.read_parquet('~/ski/elo/python/ski/excel365/M_Sprint_C.parquet')
m_sprint_f = pd.read_parquet('~/ski/elo/python/ski/excel365/M_Sprint_F.parquet')
m_c = pd.read_parquet('~/ski/elo/python/ski/excel365/M_C.parquet')
m_f = pd.read_parquet('~/ski/elo/python/ski/excel365/M_F.parquet')


l_all = pd.read_parquet('~/ski/elo/python/ski/excel365/L.parquet')
l_distance = pd.read_parquet('~/ski/elo/python/ski/excel365/L_Distance.parquet')
l_distance_c = pd.read_parquet('~/ski/elo/python/ski/excel365/L_Distance_C.parquet')
l_distance_f = pd.read_parquet('~/ski/elo/python/ski/excel365/L_Distance_F.parquet')
l_sprint = pd.read_parquet('~/ski/elo/python/ski/excel365/L_Sprint.parquet')
l_sprint_c = pd.read_parquet('~/ski/elo/python/ski/excel365/L_Sprint_C.parquet')
l_sprint_f = pd.read_parquet('~/ski/elo/python/ski/excel365/L_Sprint_F.parquet')
l_c = pd.read_parquet('~/ski/elo/python/ski/excel365/L_C.parquet')
l_f = pd.read_parquet('~/ski/elo/python/ski/excel365/L_F.parquet')
'''



#HDF5 -- 346.532, 2.942, 1.104, 4.064
m_all = pd.read_hdf('~/ski/elo/python/ski/excel365/M.h5', key='m_all')
m_distance = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Distance.h5', key='m_distance')
m_distance_c = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Distance_C.h5', key='m_distance_c')
m_distance_f = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Distance_F.h5', key='m_distance_f')
m_sprint = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Sprint.h5', key='m_sprint')
m_sprint_c = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Sprint_C.h5', key='m_sprint_c')
m_sprint_f = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Sprint_F.h5', key='m_sprint_f')
m_c = pd.read_hdf('~/ski/elo/python/ski/excel365/M_C.h5', key='m_c')
m_f = pd.read_hdf('~/ski/elo/python/ski/excel365/M_F.h5', key='m_f')


l_all = pd.read_hdf('~/ski/elo/python/ski/excel365/L.h5', key='l_all')
l_distance = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Distance.h5', key='l_distance')
l_distance_c = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Distance_C.h5', key='l_distance_c')
l_distance_f = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Distance_F.h5', key='l_distance_f')
l_sprint = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Sprint.h5', key='l_sprint')
l_sprint_c = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Sprint_C.h5', key='l_sprint_c')
l_sprint_f = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Sprint_F.h5', key='l_sprint_f')
l_c = pd.read_hdf('~/ski/elo/python/ski/excel365/L_C.h5', key='l_c')
l_f = pd.read_hdf('~/ski/elo/python/ski/excel365/L_F.h5', key='l_f')


m_all['Distance'] = m_all['Distance'].astype(str)
m_distance['Distance'] = m_distance['Distance'].astype(str)
m_distance_c['Distance'] = m_distance_c['Distance'].astype(str)
m_distance_f['Distance'] = m_distance_f['Distance'].astype(str)
m_sprint['Distance'] = m_sprint['Distance'].astype(str)
m_sprint_c['Distance'] = m_sprint_c['Distance'].astype(str)
m_sprint_f['Distance'] = m_sprint_f['Distance'].astype(str)
m_c['Distance'] = m_c['Distance'].astype(str)
m_f['Distance'] = m_f['Distance'].astype(str)

l_all['Distance'] = l_all['Distance'].astype(str)
l_distance['Distance'] = l_distance['Distance'].astype(str)
l_distance_c['Distance'] = l_distance_c['Distance'].astype(str)
l_distance_f['Distance'] = l_distance_f['Distance'].astype(str)
l_sprint['Distance'] = l_sprint['Distance'].astype(str)
l_sprint_c['Distance'] = l_sprint_c['Distance'].astype(str)
l_sprint_f['Distance'] = l_sprint_f['Distance'].astype(str)
l_c['Distance'] = l_c['Distance'].astype(str)
l_f['Distance'] = l_f['Distance'].astype(str)


m_all.to_hdf('~/ski/elo/python/ski/excel365/M.h5', key='m_all', mode='w')
m_distance.to_hdf('~/ski/elo/python/ski/excel365/M_Distance.h5', key='m_distance', mode='w')
m_distance_c.to_hdf('~/ski/elo/python/ski/excel365/M_Distance_C.h5', key='m_distance_c', mode='w')
m_distance_f.to_hdf('~/ski/elo/python/ski/excel365/M_Distance_F.h5', key='m_distance_f', mode='w')
m_sprint.to_hdf('~/ski/elo/python/ski/excel365/M_Sprint.h5', key='m_sprint', mode='w')
m_sprint_c.to_hdf('~/ski/elo/python/ski/excel365/M_Sprint_C.h5', key='m_sprint_c', mode='w')
m_sprint_f.to_hdf('~/ski/elo/python/ski/excel365/M_Sprint_F.h5', key='m_sprint_f', mode='w')
m_c.to_hdf('~/ski/elo/python/ski/excel365/M_C.h5', key='m_c', mode='w')
m_f.to_hdf('~/ski/elo/python/ski/excel365/M_F.h5', key='m_f', mode='w')

l_all.to_hdf('~/ski/elo/python/ski/excel365/L.h5', key='l_all', mode='w')
l_distance.to_hdf('~/ski/elo/python/ski/excel365/L_Distance.h5', key='l_distance', mode='w')
l_distance_c.to_hdf('~/ski/elo/python/ski/excel365/L_Distance_C.h5', key='l_distance_c', mode='w')
l_distance_f.to_hdf('~/ski/elo/python/ski/excel365/L_Distance_F.h5', key='l_distance_f', mode='w')
l_sprint.to_hdf('~/ski/elo/python/ski/excel365/L_Sprint.h5', key='l_sprint', mode='w')
l_sprint_c.to_hdf('~/ski/elo/python/ski/excel365/L_Sprint_C.h5', key='l_sprint_c', mode='w')
l_sprint_f.to_hdf('~/ski/elo/python/ski/excel365/L_Sprint_F.h5', key='l_sprint_f', mode='w')
l_c.to_hdf('~/ski/elo/python/ski/excel365/L_C.h5', key='l_c', mode='w')
l_f.to_hdf('~/ski/elo/python/ski/excel365/L_F.h5', key='l_f', mode='w')
split2 = time.time() - start_time-split1
print(split2)

m_all = pd.read_hdf('~/ski/elo/python/ski/excel365/M.h5', key='m_all')
m_distance = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Distance.h5', key='m_distance')
m_distance_c = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Distance_C.h5', key='m_distance_c')
m_distance_f = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Distance_F.h5', key='m_distance_f')
m_sprint = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Sprint.h5', key='m_sprint')
m_sprint_c = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Sprint_C.h5', key='m_sprint_c')
m_sprint_f = pd.read_hdf('~/ski/elo/python/ski/excel365/M_Sprint_F.h5', key='m_sprint_f')
m_c = pd.read_hdf('~/ski/elo/python/ski/excel365/M_C.h5', key='m_c')
m_f = pd.read_hdf('~/ski/elo/python/ski/excel365/M_F.h5', key='m_f')


l_all = pd.read_hdf('~/ski/elo/python/ski/excel365/L.h5', key='l_all')
l_distance = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Distance.h5', key='l_distance')
l_distance_c = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Distance_C.h5', key='l_distance_c')
l_distance_f = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Distance_F.h5', key='l_distance_f')
l_sprint = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Sprint.h5', key='l_sprint')
l_sprint_c = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Sprint_C.h5', key='l_sprint_c')
l_sprint_f = pd.read_hdf('~/ski/elo/python/ski/excel365/L_Sprint_F.h5', key='l_sprint_f')
l_c = pd.read_hdf('~/ski/elo/python/ski/excel365/L_C.h5', key='l_c')
l_f = pd.read_hdf('~/ski/elo/python/ski/excel365/L_F.h5', key='l_f')


#Feather -- 323.413, 0.622, 0.758, 1.380
'''m_all['Distance'] = m_all['Distance'].astype(str)
m_distance['Distance'] = m_distance['Distance'].astype(str)
m_distance_c['Distance'] = m_distance_c['Distance'].astype(str)
m_distance_f['Distance'] = m_distance_f['Distance'].astype(str)
m_sprint['Distance'] = m_sprint['Distance'].astype(str)
m_sprint_c['Distance'] = m_sprint_c['Distance'].astype(str)
m_sprint_f['Distance'] = m_sprint_f['Distance'].astype(str)
m_c['Distance'] = m_c['Distance'].astype(str)
m_f['Distance'] = m_f['Distance'].astype(str)

l_all['Distance'] = l_all['Distance'].astype(str)
l_distance['Distance'] = l_distance['Distance'].astype(str)
l_distance_c['Distance'] = l_distance_c['Distance'].astype(str)
l_distance_f['Distance'] = l_distance_f['Distance'].astype(str)
l_sprint['Distance'] = l_sprint['Distance'].astype(str)
l_sprint_c['Distance'] = l_sprint_c['Distance'].astype(str)
l_sprint_f['Distance'] = l_sprint_f['Distance'].astype(str)
l_c['Distance'] = l_c['Distance'].astype(str)
l_f['Distance'] = l_f['Distance'].astype(str)


m_all.to_feather('~/ski/elo/python/ski/excel365/M.feather')
m_distance.to_feather('~/ski/elo/python/ski/excel365/M_Distance.feather')
m_distance_c.to_feather('~/ski/elo/python/ski/excel365/M_Distance_C.feather')
m_distance_f.to_feather('~/ski/elo/python/ski/excel365/M_Distance_F.feather')
m_sprint.to_feather('~/ski/elo/python/ski/excel365/M_Sprint.feather')
m_sprint_c.to_feather('~/ski/elo/python/ski/excel365/M_Sprint_C.feather')
m_sprint_f.to_feather('~/ski/elo/python/ski/excel365/M_Sprint_F.feather')
m_c.to_feather('~/ski/elo/python/ski/excel365/M_C.feather')
m_f.to_feather('~/ski/elo/python/ski/excel365/M_F.feather')



l_all.to_feather('~/ski/elo/python/ski/excel365/L.feather')
l_distance.to_feather('~/ski/elo/python/ski/excel365/L_Distance.feather')
l_distance_c.to_feather('~/ski/elo/python/ski/excel365/L_Distance_C.feather')
l_distance_f.to_feather('~/ski/elo/python/ski/excel365/L_Distance_F.feather')
l_sprint.to_feather('~/ski/elo/python/ski/excel365/L_Sprint.feather')
l_sprint_c.to_feather('~/ski/elo/python/ski/excel365/L_Sprint_C.feather')
l_sprint_f.to_feather('~/ski/elo/python/ski/excel365/L_Sprint_F.feather')
l_c.to_feather('~/ski/elo/python/ski/excel365/L_C.feather')
l_f.to_feather('~/ski/elo/python/ski/excel365/L_F.feather')
split2 = time.time() - start_time-split1
print(split2)

m_all = pd.read_feather('~/ski/elo/python/ski/excel365/M.feather')
m_distance = pd.read_feather('~/ski/elo/python/ski/excel365/M_Distance.feather')
m_distance_c = pd.read_feather('~/ski/elo/python/ski/excel365/M_Distance_C.feather')
m_distance_f = pd.read_feather('~/ski/elo/python/ski/excel365/M_Distance_F.feather')
m_sprint = pd.read_feather('~/ski/elo/python/ski/excel365/M_Sprint.feather')
m_sprint_c = pd.read_feather('~/ski/elo/python/ski/excel365/M_Sprint_C.feather')
m_sprint_f = pd.read_feather('~/ski/elo/python/ski/excel365/M_Sprint_F.feather')
m_c = pd.read_feather('~/ski/elo/python/ski/excel365/M_C.feather')
m_f = pd.read_feather('~/ski/elo/python/ski/excel365/M_F.feather')


l_all = pd.read_feather('~/ski/elo/python/ski/excel365/L.feather')
l_distance = pd.read_feather('~/ski/elo/python/ski/excel365/L_Distance.feather')
l_distance_c = pd.read_feather('~/ski/elo/python/ski/excel365/L_Distance_C.feather')
l_distance_f = pd.read_feather('~/ski/elo/python/ski/excel365/L_Distance_F.feather')
l_sprint = pd.read_feather('~/ski/elo/python/ski/excel365/L_Sprint.feather')
l_sprint_c = pd.read_feather('~/ski/elo/python/ski/excel365/L_Sprint_C.feather')
l_sprint_f = pd.read_feather('~/ski/elo/python/ski/excel365/L_Sprint_F.feather')
l_c = pd.read_feather('~/ski/elo/python/ski/excel365/L_C.feather')
l_f = pd.read_feather('~/ski/elo/python/ski/excel365/L_F.feather')'''

#csv 289.466, 11.918, 3.368, 15.285
'''m_all.to_csv('~/ski/elo/python/ski/excel365/M.csv')
m_distance.to_csv('~/ski/elo/python/ski/excel365/M_Distance.csv')
m_distance_c.to_csv('~/ski/elo/python/ski/excel365/M_Distance_C.csv')
m_distance_f.to_csv('~/ski/elo/python/ski/excel365/M_Distance_F.csv')
m_sprint.to_csv('~/ski/elo/python/ski/excel365/M_Sprint.csv')
m_sprint_c.to_csv('~/ski/elo/python/ski/excel365/M_Sprint_C.csv')
m_sprint_f.to_csv('~/ski/elo/python/ski/excel365/M_Sprint_F.csv')
m_c.to_csv('~/ski/elo/python/ski/excel365/M_C.csv')
m_f.to_csv('~/ski/elo/python/ski/excel365/M_F.csv')

l_all.to_csv('~/ski/elo/python/ski/excel365/L.csv')
l_distance.to_csv('~/ski/elo/python/ski/excel365/L_Distance.csv')
l_distance_c.to_csv('~/ski/elo/python/ski/excel365/L_Distance_C.csv')
l_distance_f.to_csv('~/ski/elo/python/ski/excel365/L_Distance_F.csv')
l_sprint.to_csv('~/ski/elo/python/ski/excel365/L_Sprint.csv')
l_sprint_c.to_csv('~/ski/elo/python/ski/excel365/L_Sprint_C.csv')
l_sprint_f.to_csv('~/ski/elo/python/ski/excel365/L_Sprint_F.csv')
l_c.to_csv('~/ski/elo/python/ski/excel365/L_C.csv')
l_f.to_csv('~/ski/elo/python/ski/excel365/L_F.csv')
split2 = time.time() - start_time-split1
print(split2)

m_all = pd.read_csv('~/ski/elo/python/ski/excel365/M.csv')
m_distance = pd.read_csv('~/ski/elo/python/ski/excel365/M_Distance.csv')
m_distance_c = pd.read_csv('~/ski/elo/python/ski/excel365/M_Distance_C.csv')
m_distance_f = pd.read_csv('~/ski/elo/python/ski/excel365/M_Distance_F.csv')
m_sprint = pd.read_csv('~/ski/elo/python/ski/excel365/M_Sprint.csv')
m_sprint_c = pd.read_csv('~/ski/elo/python/ski/excel365/M_Sprint_C.csv')
m_sprint_f = pd.read_csv('~/ski/elo/python/ski/excel365/M_Sprint_F.csv')
m_c = pd.read_csv('~/ski/elo/python/ski/excel365/M_C.csv')
m_f = pd.read_csv('~/ski/elo/python/ski/excel365/M_F.csv')


l_all = pd.read_csv('~/ski/elo/python/ski/excel365/L.csv')
l_distance = pd.read_csv('~/ski/elo/python/ski/excel365/L_Distance.csv')
l_distance_c = pd.read_csv('~/ski/elo/python/ski/excel365/L_Distance_C.csv')
l_distance_f = pd.read_csv('~/ski/elo/python/ski/excel365/L_Distance_F.csv')
l_sprint = pd.read_csv('~/ski/elo/python/ski/excel365/L_Sprint.csv')
l_sprint_c = pd.read_csv('~/ski/elo/python/ski/excel365/L_Sprint_C.csv')
l_sprint_f = pd.read_csv('~/ski/elo/python/ski/excel365/L_Sprint_F.csv')
l_c = pd.read_csv('~/ski/elo/python/ski/excel365/L_C.csv')
l_f = pd.read_csv('~/ski/elo/python/ski/excel365/L_F.csv')'''



#pkl 331.337, 0.432, 0.325, 0.758
'''m_all.to_pickle('~/ski/elo/python/ski/excel365/M.pkl')
m_distance.to_pickle('~/ski/elo/python/ski/excel365/M_Distance.pkl')
m_distance_c.to_pickle('~/ski/elo/python/ski/excel365/M_Distance_C.pkl')
m_distance_f.to_pickle('~/ski/elo/python/ski/excel365/M_Distance_F.pkl')
m_sprint.to_pickle('~/ski/elo/python/ski/excel365/M_Sprint.pkl')
m_sprint_c.to_pickle('~/ski/elo/python/ski/excel365/M_Sprint_C.pkl')
m_sprint_f.to_pickle('~/ski/elo/python/ski/excel365/M_Sprint_F.pkl')
m_c.to_pickle('~/ski/elo/python/ski/excel365/M_C.pkl')
m_f.to_pickle('~/ski/elo/python/ski/excel365/M_F.pkl')

l_all.to_pickle('~/ski/elo/python/ski/excel365/L.pkl')
l_distance.to_pickle('~/ski/elo/python/ski/excel365/L_Distance.pkl')
l_distance_c.to_pickle('~/ski/elo/python/ski/excel365/L_Distance_C.pkl')
l_distance_f.to_pickle('~/ski/elo/python/ski/excel365/L_Distance_F.pkl')
l_sprint.to_pickle('~/ski/elo/python/ski/excel365/L_Sprint.pkl')
l_sprint_c.to_pickle('~/ski/elo/python/ski/excel365/L_Sprint_C.pkl')
l_sprint_f.to_pickle('~/ski/elo/python/ski/excel365/L_Sprint_F.pkl')
l_c.to_pickle('~/ski/elo/python/ski/excel365/L_C.pkl')
l_f.to_pickle('~/ski/elo/python/ski/excel365/L_F.pkl')
split2 = time.time() - start_time-split1
print(split2)

m_all = pd.read_pickle('~/ski/elo/python/ski/excel365/M.pkl')
m_distance = pd.read_pickle('~/ski/elo/python/ski/excel365/M_Distance.pkl')
m_distance_c = pd.read_pickle('~/ski/elo/python/ski/excel365/M_Distance_C.pkl')
m_distance_f = pd.read_pickle('~/ski/elo/python/ski/excel365/M_Distance_F.pkl')
m_sprint = pd.read_pickle('~/ski/elo/python/ski/excel365/M_Sprint.pkl')
m_sprint_c = pd.read_pickle('~/ski/elo/python/ski/excel365/M_Sprint_C.pkl')
m_sprint_f = pd.read_pickle('~/ski/elo/python/ski/excel365/M_Sprint_F.pkl')
m_c = pd.read_pickle('~/ski/elo/python/ski/excel365/M_C.pkl')
m_f = pd.read_pickle('~/ski/elo/python/ski/excel365/M_F.pkl')


l_all = pd.read_pickle('~/ski/elo/python/ski/excel365/L.pkl')
l_distance = pd.read_pickle('~/ski/elo/python/ski/excel365/L_Distance.pkl')
l_distance_c = pd.read_pickle('~/ski/elo/python/ski/excel365/L_Distance_C.pkl')
l_distance_f = pd.read_pickle('~/ski/elo/python/ski/excel365/L_Distance_F.pkl')
l_sprint = pd.read_pickle('~/ski/elo/python/ski/excel365/L_Sprint.pkl')
l_sprint_c = pd.read_pickle('~/ski/elo/python/ski/excel365/L_Sprint_C.pkl')
l_sprint_f = pd.read_pickle('~/ski/elo/python/ski/excel365/L_Sprint_F.pkl')
l_c = pd.read_pickle('~/ski/elo/python/ski/excel365/L_C.pkl')
l_f = pd.read_pickle('~/ski/elo/python/ski/excel365/L_F.pkl')'''


print(time.time() - start_time-split1-split2)
print(time.time() - start_time-split1)

#m_all.to_parquet()