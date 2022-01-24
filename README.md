# ADSDB EtE Data Science Project
This repository showcases a project completed as part of the ADSDB course at the Universitat Politecnica de Catalunya as part of the Master of Data Science. 

### Environment

This project requires at least **Python 3.8** and the following Python libraries installed:

- [pandas](https://pandas.pydata.org/docs/)
- [numpy](https://numpy.org/doc/)
- [matplotlib](https://matplotlib.org/stable/users/index.html)
- [psycopg2](https://pypi.org/project/psycopg2/)
- [sqlalchemy](https://docs.sqlalchemy.org/en/14/)

### Index
- [landing](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing)
  - [`export_to_formatted.ipynb`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/landing/export_to_formatted.ipynb)
  - [`export_to_formatted.py`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/landing/export_to_formatted.py)
  - [persistent](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent)
    - [London_weather](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather)
      - [cloud_cover](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/cloud_cover)
      - [global_radiation](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/global_radiation)
      - [max_temperature](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/max_temperature)
      - [mean_temperature](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/mean_temperature)
      - [min_temperature](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/min_temperature)
      - [precipitation_amount](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/precipitation_amount)
      - [sea_level_pressure](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/sea_level_pressure)
      - [snow_depth](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/snow_depth)
      - [sunshine](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_weather/sunshine)
    - [London_energy](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_energy)
      - [`london_energy.csv.zip`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/persistent/London_energy)
  - [temporal](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/tree/main/landing/temporal)
- [formatted](https://github.com/emmanuelfwerr/DataEngineering/tree/master/Build%20a%20Cloud%20Data%20Warehouse)
  - [`profiling_formatted.ipynb`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/formatted/profiling_formatted.ipynb)
  - [`profiling_formatted.py`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/formatted/profiling_formatted.py)
- [trusted](https://github.com/emmanuelfwerr/DataEngineering/tree/master/Build%20a%20Data%20Lake)
  - [`profiling_and_export_to_trusted.ipynb`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/trusted/profiling_and_export_to_trusted.ipynb)
  - [`profiling_and_export_to_trusted.py`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/trusted/profiling_and_export_to_trusted.py)
- [exploitation](https://github.com/emmanuelfwerr/DataEngineering/tree/master/Data%20Pipelines%20with%20Airflow)
  - [`Integration_and_export_to_exploitation.ipynb`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/exploitation/Integration_and_export_to_exploitation.ipynb)
  - [`Integration_and_export_to_exploitation.py`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/exploitation/integration_and_export_to_exploitation.py)
- [feature_generation](https://github.com/emmanuelfwerr/DataEngineering/tree/master/Data%20Pipelines%20with%20Airflow)
  - [`modelling.ipynb`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/feature_generation/modelling.ipynb)
  - [`modelling.py`](https://github.com/emmanuelfwerr/ADSDB-EtE-Data-Science-Project/blob/main/feature_generation/modelling.py)

Each folder should be accompanied by detailed documentation of operations in the form of Jupyter Notebooks!

### Data

The source data is in `.csv` and  `.txt` source files located insinde the `persistent` folder.

* Weather data: [`source`](https://www.ecad.eu/dailydata/customquery.php?optionSelected=element&processtext1=Your+query+is+being+processed.+Please+wait...&blendingselect=yes&countryselect=SPAIN%7Ces&stationselect=All+stations%7C**&elementselect=All+elements%7C**&advanced=yes&periodselect=1979-2020%7C1979-2020&elevationselect=**&processtext2=Your+query+is+being+processed.+Please+wait)
* Energy data: [`source`](https://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households)
    
The first dataset contains daily weather measurements from a weather station in Heathrow Airport next to London. This is the closest available weather station near London. Data is spread out in different folders depending on their measurement. They are organized this way to facilitate adding extra source files for any measurement at a time in the future without breakaing code.

The second dataset consists of energy consumption data for every half hour of every day between 2011-11-23 and 2014-02-28 fir 5667 different homes. This data is provided by a London power company who was, at the time, performing a usage vs pricing experiment on a subset of their customers.

### Instructions to Run

* Download all files to your own computer while making sure to maintain file and directory hierarchy
* Must download `London_energy` from source link provided above and add it to /landing/London_energy
* Run `orchestrator.py`
    * This will run each of the scripts in each zone sequentially thus completing flow of data from source to final tables

