import pandas as pd
f=pd.read_csv("green_tripdata_2016-01.csv")
keep_col = ['Pickup_longitude','Pickup_latitude']
new_f = f[keep_col]
new_f.to_csv("newFile.csv", index=False)
