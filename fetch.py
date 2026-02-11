import requests
import pandas as pd
from datetime import datetime

import pymysql
import sqlalchemy as sqlalchemy
from sqlalchemy import create_engine


url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

all_records = []
start_year = datetime.now().year - 5   # last 5 years
end_year = datetime.now().year

for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1}-01-01"
        else:
            end_date = f"{year}-{month+1:02d}-01"

        params = {
            "format": "geojson",
            "starttime": start_date,
            "endtime": end_date,
            "minmagnitude": 3
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"⚠️ Failed for {start_date}: {response.text[:200]}")
            continue

        try:
            data = response.json()
        except Exception as e:
            print(f"⚠️ JSON error for {start_date}: {e}")
            continue

        for f in data["features"]:
            p = f["properties"]
            g = f["geometry"]["coordinates"]
            all_records.append({
                "id": f.get("id"),
                "time": pd.to_datetime(p.get("time"), unit="ms"),
                "updated": pd.to_datetime(p.get("updated"), unit="ms"),
                "latitude": g[1] if g else None,
                "longitude": g[0] if g else None,
                "depth_km": g[2] if g else None,
                "mag":p.get("mag"),
                "magType":p.get("magType"),
                "alert":p.get("alert"), 
                "felt":p.get("felt"), 
                "cdi":p.get("cdi"),
                "mmi":p.get("mmi"),
                "code":p.get("code"),
                "place":p.get("place"),
                "status":p.get("status"),
                "tsunami":p.get("tsunami"),
                " sig":p.get("sig"),
                "net":p.get("net"),
                "nst":p.get("nst"),
                "dmin":p.get("dmin"),
                "rms":p.get("rms"),
                "gap":p.get("gap"),
                "types":p.get("types"),
                "ids":p.get("ids"),
                "sources":p.get("sources"),
                "type":p.get("type")
            








              # Event type
           })
            
df = pd.DataFrame(all_records)

df_csv=df.to_csv("fetch.csv", index=False)
df["alert"]=df["alert"].fillna(("green"))
df["felt"]=df["felt"].fillna(df["felt"].mean())
df["cdi"]=df["cdi"].fillna(df["cdi"].mean())
df["mmi"]=df["mmi"].fillna(df["mmi"].mean())
df["nst"]=df["nst"].fillna(df["nst"].mean())
df["dmin"]=df["rms"].fillna(df["rms"].mean())
df["rms"]=df["rms"].fillna(df["rms"].mean())
df["gap"]=df["gap"].fillna(df["gap"].mean())

print(df)

try:
    # Connection Parameters
    connection1 = pymysql.connect(
         host = 'localhost', user = 'root', 
         password = '12345', database = 'earthquakes')
    print("connection = ", connection1)
    cursor = connection1.cursor()
    print("cursor = ", cursor)  
    
except Exception as e:
    print(str(e))


engine=create_engine("mysql+pymysql://root:12345@localhost/earthquakes")

df.to_sql(
    name='records',        # table name
    con=engine,
    if_exists='append',     # insert data (no overwrite)
    index=False             # avoid DataFrame index column
)

print('data inserted sucessfully')
