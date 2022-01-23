#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import psycopg2


# ## Extracting weather data from persistent folder in landing zone
# 
# ### Weather Data Summary
# - `SOUID` refers to weather station ID and has 2 unique values
#     - '116436' and '903772' both refer to weather stations near Heathrow Airport
# - `DATE` refers to date of wather measurement
#     - measurements range from '1979-01-01' to '2020-12-31'
# - `CC` cloud cover measurement in **oktas**
# - `SS` sunshine measurement in **0.1 Hours**
# - `QQ` global radiation measurement in **W/m2**
# - `TX` maximum temperature measurement in **0.1 °C**
# - `TG` mean temperature measurement in **0.1 °C**
# - `TN` minimum temperature measurement in **0.1 °C**
# - `RR` precipitation measurement scaled in **0.1 mm**
# - `PP` pressure measurement in **0.1 hPa**
# - `SD` snow depth measurement in **1 cm**
# - `Q_` refers to quality of weather measurement
#     - from trusted (0), to dubious (1), and incorrect (9)
# 
# **Missing Values:**
# - Measurements that are missing are marked as -9999 and have `Q_` of 9

# In[2]:


# Importing London weather data
cloud_cover_df = pd.read_csv('persistent/London_weather/cloud_cover/CC_STAID001860.txt', sep=",")
sunshine_df = pd.read_csv('persistent/london_weather/sunshine/SS_STAID001860.txt', sep=",")
global_radiation_df = pd.read_csv('persistent/london_weather/global_radiation/QQ_STAID001860.txt', sep=",")
max_temp_df = pd.read_csv('persistent/london_weather/max_temperature/TX_STAID001860.txt', sep=",")
mean_temp_df = pd.read_csv('persistent/london_weather/mean_temperature/TG_STAID001860.txt', sep=",")
min_temp_df = pd.read_csv('persistent/london_weather/min_temperature/TN_STAID001860.txt', sep=",")
precipitation_df = pd.read_csv('persistent/london_weather/precipitation_amount/RR_STAID001860.txt', sep=",")
pressure_df = pd.read_csv('persistent/london_weather/sea_level_pressure/PP_STAID001860.txt', sep=",")
snow_depth_df = pd.read_csv('persistent/london_weather/snow_depth/SD_STAID001860.txt', sep=",")


# ### Merging all weather measurement dataframes
# This will be done to simplify exporting to PostgreSQL as well as simplifying data flow moving forward. The process is simple because all dataframes have same range of date values as well as valid values for each date. This ensures resulting dataframe will be complete
# 
# **Issues to fix:**
# - Column names are formatted improperly with whitespaces
#     - Discrepancies in names are the same for all dataframes
#     - This will be fixed as a preliminary step for all dataframes
# - Missing values are represented by -9999
#     - They will be assigned NaN
# - Measurements have different units and scalings
#     - These will be treated in the trusted zone

# In[3]:


# formatting column names correctly
cloud_cover_df = cloud_cover_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   CC': 'CC', ' Q_CC': 'Q_CC'}, axis=1)  # cloud cover
sunshine_df = sunshine_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   SS': 'SS', ' Q_SS': 'Q_SS'}, axis=1)  # sunshine
global_radiation_df = global_radiation_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   QQ': 'QQ', ' Q_QQ': 'Q_QQ'}, axis=1)  # global radiation
max_temp_df = max_temp_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   TX': 'TX', ' Q_TX': 'Q_TX'}, axis=1)  # max temp
mean_temp_df = mean_temp_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   TG': 'TG', ' Q_TG': 'Q_TG'}, axis=1)  # mean temp
min_temp_df = min_temp_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   TN': 'TN', ' Q_TN': 'Q_TN'}, axis=1)  # min temp
precipitation_df = precipitation_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   RR': 'RR', ' Q_RR': 'Q_RR'}, axis=1)  # precipitation
pressure_df = pressure_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   PP': 'PP', ' Q_PP': 'Q_PP'}, axis=1)  # pressure
snow_depth_df = snow_depth_df.rename({' SOUID': 'SOUID', '    DATE': 'DATE', '   SD': 'SD', ' Q_SD': 'Q_SD'}, axis=1)  # snow depth

# dropping SOUID from all dataframes
cloud_cover_df = cloud_cover_df.drop(['SOUID'], axis=1)
sunshine_df = sunshine_df.drop(['SOUID'], axis=1)
global_radiation_df = global_radiation_df.drop(['SOUID'], axis=1)
max_temp_df = max_temp_df.drop(['SOUID'], axis=1)
mean_temp_df = mean_temp_df.drop(['SOUID'], axis=1)
min_temp_df = min_temp_df.drop(['SOUID'], axis=1)
precipitation_df = precipitation_df.drop(['SOUID'], axis=1)
pressure_df = pressure_df.drop(['SOUID'], axis=1)
snow_depth_df = snow_depth_df.drop(['SOUID'], axis=1)

# merging into single dataframe
weather_df = cloud_cover_df.merge(sunshine_df, on='DATE')
weather_df = weather_df.merge(global_radiation_df, on='DATE')
weather_df = weather_df.merge(max_temp_df, on='DATE')
weather_df = weather_df.merge(mean_temp_df, on='DATE')
weather_df = weather_df.merge(min_temp_df, on='DATE')
weather_df = weather_df.merge(precipitation_df, on='DATE')
weather_df = weather_df.merge(pressure_df, on='DATE')
weather_df = weather_df.merge(snow_depth_df, on='DATE')

# replacing -9999 values with NaN
weather_df['CC'] = weather_df['CC'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['SS'] = weather_df['SS'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['QQ'] = weather_df['QQ'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['TX'] = weather_df['TX'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['TG'] = weather_df['TG'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['TN'] = weather_df['TN'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['RR'] = weather_df['RR'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['PP'] = weather_df['PP'].map(lambda x: np.nan if x in [-9999] else x)
weather_df['SD'] = weather_df['SD'].map(lambda x: np.nan if x in [-9999] else x)


# In[4]:


weather_df


# ### Exporting Weather Data to Formatted Zone Database in PostgreSQL

# In[5]:


from sqlalchemy import create_engine

# Create an engine instance
conn_string = 'postgresql://postgres:ETS80321123GOM1!@localhost:5432/formatted_zone'
db = create_engine(conn_string)

# Connect to PostgreSQL server
conn = db.connect()

# Load weather data from dataframe into PostgreSQL database table named weather
weather_df.to_sql('weather', con=conn, if_exists='replace', index=False)

# conn.commit()
conn.close()


# #### If we would like to query our new table we could use the code below

# In[ ]:


# Connect to PostgreSQL server
conn = psycopg2.connect(conn_string)
conn.set_session(autocommit=True) # autocommit

# Instantiating cursor
cur = conn.cursor()

# Your SQL query below...
sql1 = '''select DATE, count(*) from weather;'''
cur.execute(sql1)
for i in cur.fetchall():
    print(i)

# closing connection
conn.close()


# ## Extracting energy data from persistent folder in landing zone
# 
# ### Energy Data Summary
# - `LCLid` refers to household ID and has 5566 unique values
# - `stdorToU` refers o whether house is in standard sub-category or in the ToT category which keeps track of energy consumption cost
#     - there are 2 unique values: **std** and **ToU**
# - `DateTime` refers to date and time of energy consumption measurement
#     - measurements range from '2011-11-01' to '2014-02-28'
# - `KWH/hh` refers to energy expenditure measured in KWH per half hour
#     - This is a continuous variable

# In[4]:


# Importing London energy data
energy_df = pd.read_csv('persistent/London_energy/London_energy.csv')


# ### Summarizing Energy Data
# Raw dataset contains energy consumption per household for every half hour. We will condense this down to daily consumption by summing half hour consumption by household and by date. This will be done in order to reduce the size of the dataset (8Gb) as well as simplifying flow of data moving forward. The dataset also needs to be condensed to daily values in order to be compatible with daily weather data.
# 
# **Issues to fix:**
# - `KWH` column has wrongly formatted name with whitespaces
# - `KWH` values are chr type
#     - They will be changed to float
# - `DateTime` column will be split into 'Date' and 'Time'
#     - `Date` will be kept in order to aggregate data
#     - `Time` will be dropped in order to properly aggregate data by Date

# In[41]:


# renaming columns
energy_df.rename({'KWH/hh (per half hour) ': 'KWH'}, axis=1, inplace=True)

# setting chr values in 'KWH' to np.nan
energy_df['KWH'] = energy_df['KWH'].map(lambda x: np.nan if x in ['Null', ' '] else x)
# setting 'KWH' type to float
energy_df['KWH'] = energy_df['KWH'].astype(float)

# splitting 'DateTime' into separate 'Date' and 'Time'
energy_df['Date'] = pd.to_datetime(energy_df['DateTime']).dt.date
energy_df['Time'] = pd.to_datetime(energy_df['DateTime']).dt.time

# Dropping 'Time' and 'DateTime' from energy_df
energy_df_byday = energy_df.drop(['Time', 'DateTime', 'stdorToU'], axis=1)

# Sum of KWH by 'LCLid' and 'Date'
sum_energy_df_byday = energy_df_byday.groupby(['LCLid','Date']).sum().reset_index()


# ### Exporting Energy Data to Formatted Zone Database in PostgreSQL

# In[ ]:


from sqlalchemy import create_engine

# Create an engine instance
conn_string = 'postgresql://postgres:ETS80321123GOM1!@localhost:5432/formatted_zone'
db = create_engine(conn_string)

# Connect to PostgreSQL server
conn = db.connect()

# Load energy data from dataframe into PostgreSQL database table named energy
sum_energy_df_byday.to_sql('energy', con=conn, if_exists='replace', index=False)

# conn.commit()
conn.close()

