# San Diego Zillow Housing Data Pipeline Project


### Why I Chose to Study San Diego Housing Market Data?

If you've been following the US real estate market during COVID-19, you've seen that the price has risen sharply. San Diego's no different, with remote work boosting house prices as people move to the city. Limited housing inventory adds to the competition. Looking to buy in San Diego? This project gathers all available properties for sale, making your search easier.

### Project Summary

* This pipeline fetches all San Diego property data from Zillow.com using Rapid API.
* The data is stored in an Amazon S3 bucket in CSV format after cleaning and transformation with Python. 
* Snowpipe automatically loads this data into a Snowflake warehouse for SQL analysis.
* Finally, Tableau is used to visualize the properties for sale, showcasing details like price, bathrooms, bedrooms, and address.

### Tools & Technologies

* __Airflow__: Data Orchestration
* __Python__: Requests data from Zillow API and processes & transform it
* __Amazon S3__: Cloud Storage for raw data and processed zillow 
* __Snowflake & Snowpipe__: Automates data flow from S3 to data warehouse and enables data analysis in data warehouse
* __Tableau__: Visualizes all sales properties based on dynamic parameters selected


### Diagram of the Data Pipeline
![Pipeline Diagram](https://github.com/shumeng-ops/San-Diego-Zillow-API-Data-Pipeline/blob/main/visualization/data%20pipeline.gif)

### Visualization


### Technical Details

