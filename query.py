import pymysql
import pandas as pd
import streamlit as st
import sqlalchemy as sqlalchemy
from sqlalchemy import create_engine

try:
    # Connection Parameters
    connection1 = pymysql.connect(
         host = 'localhost', user = 'root', 
         password = '12345', database = 'earthquakes')
    print("connection = ", connection1)
    cursor = connection1.cursor()
    print("cursor = ", cursor) 
    
    engine=create_engine("mysql+pymysql://root:12345@localhost/earthquakes")
    
    queries= {
        " 1.Top 10 strongest earthquakes (mag)":
         """select mag,place from records order by mag desc LIMIT 10""",
    
        "2. Top 10 deepest earthquakes (depth_km)":
         """select place,depth_km from records  order by depth_km desc limit 10""",
  
        "3. Shallow earthquakes < 50 km and mag > 7.5":
          """select * from records where depth_km < 50 AND mag > 7.5""",

        "5. Average magnitude per magnitude type (magType)":
            """select magType, avg(mag) as avg_mag from records group by magType""",
        
        "6.Year with most earthquakes":
            """SELECT extract(year from  time)  year, count(*) AS earthquake_count from records group by year order by earthquake_count desc""",
    
        "7. Month with highest number of earthquakes":
            """select month(time) as Earthquake_month, count(*) aS TotalEarthquakes from records group by month(time) order by TotalEarthquakes desc""",
   
        "8. Day of week with most earthquakes":
            """select dayname(time) AS dayofweek, count(*) as EarthquakeCount from records group by dayname(time) order by earthquakeCount desc""",
    
       "9.Count of earthquakes per hour of day":
           """select extract(hour from time) as hour_of_day,count(*) as earthquake_count from records group by extract(hour from time)""",
           
       "10.Most active reporting network (net)":
           """select max(net),count(*) as active_network from records group by net order by active_network desc""",
    
       "11.Top 5 places with highest casualties":
           """select place ,sum(felt) as total_casualties from records group by place order by total_casualties desc limit 5""",
           

       "14. Count of reviewed vs automatic earthquakes (status)":
            """select status, count(*) AS count from records group by status """,
 
       "15.Count by earthquake type (type)":
            """select type ,count(*) as earthquake_count from records group by type""",
   
       "16.Number of earthquakes by data type (types)":
            """select type, countT(*) as number_of_earthquakes from records group by type order by number_of_earthquakes desc""",
    
       "18.Events with high station coverage (nst > threshold)":
           """select mag, time, nst from records where  nst > 100 order by nst desc""",
 
    
       "19.Number of tsunamis triggered per year":
         """select extract(year from time) as tsunami_year, count(*) as total_tsunamis from records group by extract(year from time) order by tsunami_year desc""",
   
    
      "20.Count earthquakes by alert levels (red, orange, etc)":
         """select alert,count(*) as earthquake_count from records group by alert""",
   
      "21.Find the top 5 countries with the highest average magnitude of earthquakes in the past 5 years":
         """select place, avg(mag) as avg_mag from records group by place order by avg_mag desc limit 5;""",
   
    " 22.Find countries that have experienced both shallow and deep earthquakes within the same month":
         """select place ,date_format(time, '%m') as month from records group by place, date_format(time, '%m') 
         having_sum(case when depth_km < 70 then 1 else 0 end) > 0 and sum(case when depth_km >= 300 then 1 else 0 end) > 0""",
         
    "23.Compute the year-over-year growth rate in the total number of earthquakes globally.":
         """select year, total,lag(total) over (order by year) as previous_year,
         round(((total - LAG(total) over (order by year)) / lag(total) over (order by year)) * 100, 2) as growth_rate
         from (select year(time) as year, count(*) as total from records group by year) as yearly""",
    
    "24. List the 3 most seismically active regions by combining both frequency and average magnitude":
         """select place,count(*) as frequency,avg(mag) as avg_mag,(count(*) * avg(mag)) as score from records group by place order by score desc limit 3""",

    "25.  For each country, calculate the average depth of earthquakes within ±5° latitude range of the equator":
          """"select place, avg(depth_km) as average_depth from records where latitude between -5 and 5 group by place""",
    
     "26. Identify countries having the highest ratio of shallow to deep earthquakes.":
         """select place, count(case when depth_km <= 70 then 1 end) * 1.0 / 
           nullif(count(case when depth_km > 70 then 1 end), 0) as shallow_to_deep_ratio from records group by place 
           order by shallow_to_deep_ratio desc""",
  
    " 27. Find the average magnitude difference between earthquakes with tsunami alerts and those without.":
    """select
		(select avg(mag) from records where tsunami = 1) as tsunami_avg,
		(select avg(mag) from records where tsunami = 0) as no_tsunami_avg,
        (select avg(mag) from records where tsunami = 1) -
	    (select avg(mag) from records where tsunami = 0) as difference""",
     
    "28. Using the gap and rms columns, identify events with the lowest data reliability (highest average error margins).":  
         """select * from records order by gap desc,rms desc""",
    
    "30. Determine the regions with the highest frequency of deep-focus earthquakes (depth > 300 km).":
        """select place, count(*) as earthquake_frequency from records where depth_km > 300 group by place order by earthquake_frequency desc"""
    
 }
    
except Exception as e:
    print(str(e))
    
    
st.title("Earthquake Data Analysis Dashboard")
 
task = st.selectbox("choose query number", list(queries.keys()))

if st.button("run query"):
    query=queries[task]
    df=pd.read_sql(query,engine)
    
    st.subheader(f"results for: {task}")
    st.dataframe(df, use_container_width = True)
    
    