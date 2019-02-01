#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 31 15:10:54 2019

@author: michael
"""

import pandas as pd
from shapely.geometry import Point, MultiPoint
from shapely.ops import nearest_points


df = pd.read_csv("./region_coordinate_files/south_asia-mainland/comid_lat_lon_z.csv", 
                 sep=',', header=0, index_col=0)
#t = MultiPoint(df.head().iloc[:, 0:2]))
df1 = df.loc[:,"Lat":"Lon"].apply(Point,axis=1,)
print(df1[df1 == d])

mt_point = MultiPoint(df1.tolist())
point = Point(36.891,75.178)

d = nearest_points(point, mt_point)
##def shortest_dist(coor, df):
##    shortest_dist = None
##    index = None
##    for i in df.index:
##        dist = ((coor[0] - df.loc[i]["Lat"])**2 + (coor[1] - df.loc[i]["Lon"])**2)**0.5
##        print(dist,i)
##        if shortest_dist == None or dist < shortest_dist:
##            shortest_dist = dist
##            index = i
##    return [shortest_dist, index]
##
##coor = [36.891,75.179]
##d = shortest_dist(coor, df.head())
print(d[1])
print(df1.head()[df1.head() == d[1]].index[0])
        