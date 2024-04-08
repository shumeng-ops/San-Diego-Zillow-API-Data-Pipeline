# San Diego Zillow Housing Data Pipeline Project

### Why Study the Housing Marketing data

If you've been following the US real estate market during COVID-19, you've seen the action. San Diego's no different, with remote work boosting house prices as people flock to the city. Limited housing inventory adds to the competition. Looking to buy in San Diego? This project gathers all available properties for sale, making your search easier.

### Project Skeleton

* This pipeline fetches property data from Zillow.com using its API.
* The data is stored in an Amazon S3 bucket in CSV format after cleaning and transformation with Python. 
* Snowpipe automatically loads this data into a Snowflake warehouse for SQL analysis.
* Finally, Tableau is used to visualize the properties for sale, showcasing details like price, bathrooms, bedrooms, and address.

### Data Pipeline Diagram
![Pipeline Diagram](/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/flow_chart/dataflow git.gif)



### Visualization


### Technical Details

