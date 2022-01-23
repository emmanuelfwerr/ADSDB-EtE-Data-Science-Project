#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
from sqlalchemy import create_engine
import psycopg2


# ## Basic Profiling of Weather and Energy tables from Formatted Zone in PostgreSQL

# In[4]:


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

# Print the DataFrame
print(weather_df.head())
print('-'*20)
print(energy_df.head())

# Close the database connection
conn.close()


# ---
# ### Weather

# In[9]:


# preview head and tail of weather_df
weather_df.head()


# In[8]:


# basic profiling of weather_df
print(weather_df.info())
print(weather_df.describe())


# ---
# ### Energy

# In[10]:


# preview head and tail of energy_df
energy_df


# In[11]:


# basic profiling of energy_df
print(energy_df.info())
print(energy_df.describe())


# ---

# In[ ]:




