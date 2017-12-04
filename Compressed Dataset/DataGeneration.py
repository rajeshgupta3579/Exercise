import pandas as pd
f=pd.read_csv("dataset.csv")
keep_col = ['Dropoff_longitude','Dropoff_latitude']
new_f = f[keep_col]
new_f.to_csv("DriverLocation.csv", index=False)
