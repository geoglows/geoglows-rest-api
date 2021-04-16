import pandas as pd
import glob
import pprint


csvs = glob.glob('/Users/rchales/code/geoglows-rest-api/GSP_API/geometry/region_coordinate_files/*-geoglows/*.csv')
pprint.pprint(csvs)

a = pd.DataFrame(columns=('COMID', 'Lat', 'Lon', 'Elev_m'))

for csv in csvs:
    a = pd.concat([a, pd.read_csv(csv)])
del a['Elev_m']
a.to_csv('/Users/rchales/code/geoglows-rest-api/GSP_API/geometry/region_coordinate_files/global_coordinate_file.csv')

a['COMID'] = a['COMID'].astype(int)
a['Lat'] = a['Lat'].astype(float)
a['Lon'] = a['Lon'].astype(float)
a = a.round(4)
print(a)


# a = pd.read_csv(
#     '/Users/rchales/code/geoglows-rest-api/GSP_API/geometry/region_coordinate_files/global_coordinate_file.csv',
#     dtype={'COMID': int, 'Lat': float, 'Lon': float, 'Elev_m': int},
#     header=0
# )
# print(a)
