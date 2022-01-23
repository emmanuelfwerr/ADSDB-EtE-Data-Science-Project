#!/usr/bin/env python
# coding: utf-8

# In[2]:


from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# ## Importing Weather and Energy Tables from Formatted Zone in PostgreSQL

# In[3]:


# Create an engine instance
conn_string = 'postgresql://postgres:****************@localhost:5432/formatted_zone'
db = create_engine(conn_string)

# Connect to PostgreSQL server
conn = db.connect()

# Read weather data from PostgreSQL database table and load into weather_df
weather_df = pd.read_sql('select * from "weather"', conn)
pd.set_option('display.expand_frame_repr', False)

# Read energy data from PostgreSQL database table and load into energy_df
energy_df = pd.read_sql('select * from "energy"', conn)
pd.set_option('display.expand_frame_repr', False)

# Print DataFrame Summaries
print('-'*30 + '\n' + 'Weather Data Summary\n' + '-'*30)
print(weather_df.info())
print(weather_df.head())

print('\n'*2 + '-'*30 + '\n' + 'Energy Data Summary\n' + '-'*30)
print(energy_df.info())
print(energy_df.head())

# Close the database connection
conn.close()


# ---
# ## Weather Data Quality and Profiling
# ### Weather Data Summary
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
# ### Missing Observations and Basic Profiling by Variable

# In[4]:


# missing observations
print('-'*30 + '\n' + 'Missing Observations by Column\n' + '-'*30)
print(weather_df.isna().sum())
# basic data profile for each variable
print('-'*30 + '\n' + 'Basic Data Profile by Column\n' + '-'*30)
weather_profile = weather_df.describe()
print(weather_profile)
# preview head and tail of weather_df
print('-'*30 + '\n' + 'Data Head & Tail Preview\n' + '-'*30)
weather_df


# 
# ### Outlier Detection
# Boxplots are shown below for different groups of weather measurements
# - Sunshine & Radiation
#     - No outliers present
# - Cloud cover
#     - No outliers present
# - Temperature
#     - Some outliers present in all temperature measurements
#         - Top and Bottom edges of max_temp (TX)
#         - Bottom edge of mean_temp (TG)
#         - Bottom edge of min_temp (TN)
# - Pressure
#     - Some outliers present
#         - Top and Bottom edges of pressure (PP)
# - Precipitation
#     - Some outliers present
#         - Bottom edges of pressure (RR)
# - Snow Depth
#     - Some outliers present
#         - Bottom edges of pressure (SD)

# In[5]:


# setting figsize
sns.set(rc = {'figure.figsize':(12,12)})

# Sunshine & Radiation Outlier Profiling
sunradiation_df = weather_df[['SS', 'QQ']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=sunradiation_df)


# In[6]:


# Cloud Cover Outlier Profiling
cloud_df = weather_df[['CC']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=cloud_df)


# In[7]:


# Temperature Outlier Profiling
temperature_df = weather_df[['TX', 'TG', 'TN']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=temperature_df)


# In[8]:


# Pressure Outlier Profiling
pressure_df = weather_df[['PP']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=pressure_df)


# In[9]:


# Precipitation Outlier Profiling
precipitation_df = weather_df[['RR']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=precipitation_df)


# In[10]:


# Snow Depth Outlier Profiling
snow_df = weather_df[['SD']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=snow_df)


# #### Missing value treatment
# - `Cloud Cover`:
#     - 19 Missing
#         - NaNs will be replaced with mean of measurement
# - `Global Radiation`:
#     - 19 Missing
#          - NaNs will be replaced with mean of measurement
# - `Max Temperature`:
#     - 6 Missing
#          - NaNs will be replaced with mean of measurement
# - `Mean Temperature`:
#     - 36 Missing
#          - NaNs will be replaced with mean of measurement
# - `Min Temperature`:
#     - 2 Missing
#          - NaNs will be replaced with mean of measurement
# - `Precipitation`:
#     - 6 Missing
#          - NaNs will be replaced with mean of measurement
# - `Pressure`:
#     - 4 Missing
#          - NaNs will be replaced with mean of measurement
# - `Snow depth`:
#     - 1441 Missing
#         - given the distribution for this variable, number of outliers, and amount of missing values. It is determined that it would be best to remove it from the dataset. In the end the expected correlation that this value would have with more energy consumption is due to the cold temperature, which we already have present in dataset.

# In[11]:


# replacing NaNs with mean of their respective column
# cloud cover
weather_df['CC'].fillna(weather_profile['CC']['mean'],inplace=True)
# global radiation
weather_df['QQ'].fillna(weather_profile['QQ']['mean'],inplace=True)
# max temperature
weather_df['TX'].fillna(weather_profile['TX']['mean'],inplace=True)
# mean temperature
weather_df['TG'].fillna(weather_profile['TG']['mean'],inplace=True)
# min temperature
weather_df['TN'].fillna(weather_profile['TN']['mean'],inplace=True)
# precipitation
weather_df['RR'].fillna(weather_profile['RR']['mean'],inplace=True)
# pressure
weather_df['PP'].fillna(weather_profile['PP']['mean'],inplace=True)

# removing `Snow Depth` from Dataset
weather_df = weather_df.drop(['SD', 'Q_SD'], axis=1)


# #### Cheking results of missing value treatment

# In[12]:


# checking results
weather_df.isna().sum()


# #### Outlier treatment
# - `Max Temperature`:
#     - Small amount of outliers are in accordance with real plausible values so they will be left untouched
# - `Mean Temperature`:
#     - Small amount of outliers are in accordance with real plausible values so they will be left untouched
# - `Min Temperature`:
#     - Small amount of outliers are in accordance with real plausible values so they will be left untouched
# - `Precipitation`:
#     - Severe Outliers will be replaced with mean of measurement
# - `Pressure`:
#     - Severe Outliers will be replaced with mean of measurement

# In[13]:


# Identifying Precipitation outliers using IQR
q1 = weather_profile['RR']['25%']
q3 = weather_profile['RR']['75%']
IQR = q3 - q1
weather_df.loc[weather_df.RR > (q3 + 3*IQR), 'RR'] = np.nan
weather_df.fillna(weather_profile['RR']['mean'],inplace=True)

# Identifying Pressure outliers using IQR
q1 = weather_profile['PP']['25%']
q3 = weather_profile['PP']['75%']
IQR = q3 - q1
weather_df.loc[weather_df.PP > (q3 + 3*IQR), 'PP'] = np.nan
weather_df.loc[weather_df.PP < (q3 - 3*IQR), 'PP'] = np.nan
weather_df.fillna(weather_profile['PP']['mean'],inplace=True)


# #### Checking results of outlier treatment
# - `Precipitation`:
#     - Mild outliers still present but are in accordance with plausible values
# - `Pressure`:
#     - Mild outliers still present but are in accordance with plausible values

# In[14]:


# Precipitation Outlier Profiling
precipitation_df = weather_df[['RR']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=precipitation_df)


# In[15]:


# Pressure Outlier Profiling
pressure_df = weather_df[['PP']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=pressure_df)


# ---
# ## Energy Data Quality and Profiling
# 
# ### Missing Observations and Basic Profiling by Variable
# 

# In[16]:


# missing observations
print('-'*30 + '\n' + 'Missing Observations by Column\n' + '-'*30)
print(energy_df.isna().sum())
# basic data profile for each variable
print('-'*30 + '\n' + 'Basic Data Profile by Column\n' + '-'*30)
energy_profile = energy_df.describe()
print(energy_profile)
# preview head and tail of weather_df
print('-'*30 + '\n' + 'Data Head & Tail Preview\n' + '-'*30)
energy_df


# ### Outlier Detection
# Boxplots are shown below for kWh measurements
# - kWh
#     - Outliers are present
#         - Top edge of KWH

# In[17]:


# Energy Consumption Outlier Profiling by day
sns.set(rc = {'figure.figsize':(12,10)})
KWH_df = energy_df[['KWH']]
sns.set_theme(style="whitegrid")
sns.boxplot(data=KWH_df)


# In[18]:


# Dropping 'LCLid' from energy_df
energy_df_byday = energy_df.drop(['LCLid'], axis=1)
# Sum of KWH by 'Date'
sum_energy_df_byday = energy_df_byday.groupby(['Date']).sum().reset_index()

# Energy Consumption Aggregate Outlier Profiling by day (5567 total homes)
sns.set(rc = {'figure.figsize':(12,10)})
sns.set_theme(style="whitegrid")
sns.boxplot(data=sum_energy_df_byday)


# #### Outlier Treatment
# Given that the average yearly energy consumption per household in the UK is around 4600 kWh (which would equate to around 12.3 kWh per day), it seems highliy unlikely that the outlier values ranging from 25kWh-350kWh are correct. Given the simplicity of this project we will simply identify the observations for these values and change their values to the mean of kWh daily consumption acrros the dataset. Only severe outliers will be treated this way, while mild outliers will be left untouched to allow for some of the original diversity in the dataset. This is not an ideal option moving forward, but given the simplicity of the project analysis, we will go ahead with that.
# 
# **However:**
# - The boxplot for aggregated sum of energy consumption of all households by day has no outliers and nearly normal distribution. As well as range of values in accordance with the estimated avg daily energy consumption for 5567 homes consuming an avg. amount of energy. So let's see how treating the dataset's sever outliers affect the overall yearly distribution.
# 
# **Result:**
# - After treatment of values (decreasing total energy consumption), the aggregated yearly energy consumption is now riddled with outliers in the bottom edge of KWH. This leads us to believe that the initial individual distribution was correct to begin with (simply with some homes over expending a large amount of energy), because in the end the aggregated energy consumption per day is in accordance with what would be expected yearly for around 5500 households.
# 
# **Reasoning:**
# - Given that in order to perform our analysis we will aggregate the energy consumption of individual households per day, it is more beneficial for us to leave the dataset as was delivered. with individual outliers (given that the outliers, when aggregated, seem to account for the average yearly energy expenditure for that sample of the population of UK.

# ---
# ## Standardizing Units, Data Types, and Formatting Column Names to be More Descriptive
# ### Weather Data Summary
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

# In[19]:


# preview weather before changes
weather_df.head()


# In[20]:


# changing 'DATE' data type to date
weather_df['DATE'] = pd.to_datetime(weather_df['DATE'], format='%Y%m%d')

# formatting column names to be more descriptive
weather_df = weather_df.rename({'DATE': 'date', 'CC': 'cloud_cover', 'SS': 'sunshine', 'QQ': 'glob_radiation', 'TX': 'max_temp', 'TG': 'mean_temp', 'TN': 'min_temp', 'RR': 'precipitation', 'PP': 'pressure'}, axis=1)  # all measurements


# In[21]:


# preview weather after changes
weather_df.head()


# ---
# ## Exporting Results to Trusted Zone in PostgreSQL

# In[22]:


# Create an engine instance
conn_string = 'postgresql://postgres:****************@localhost:5432/trusted_zone'
db = create_engine(conn_string)

# Connect to PostgreSQL server
conn = db.connect()

# Load weather data from dataframe into PostgreSQL database table named weather
weather_df.to_sql('weather', con=conn, if_exists='replace', index=False)

# Load energy data from dataframe into PostgreSQL database table named energy
energy_df.to_sql('energy', con=conn, if_exists='replace', index=False)

# conn.commit()
conn.close()


# In[ ]:




