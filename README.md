# San Diego Zillow Housing Data Pipeline Project

### Project Summary

If you've been keeping an eye on the US real estate market, you probably know it's been quite a show since COVID-19 began. And San Diego is no different. With more people working remotely, lots are moving to 'America's Finest City,' pushing house prices up. And the limited number of available homes in San Diego has heightened competition. If you're someone interested in buying a property in San Diego, this project will gather all the available housing properties for sale, making it easier for you to review them.


This data pipeline pulls information about properties for sale and recently sold properties from Zillow.com using the Zillow API. The extracted data is stored in an Amazon S3 bucket in CSV format. After cleaning and transforming the data using Python, it's stored back in the S3 bucket. Then, Snowpipe automatically loads the data from S3 into a Snowflake warehouse, where SQL analysis is performed. Finally, Tableau's spatial functionalities are utilized to create clear visualizations of the properties for sale, including details such as price, number of bathrooms and bedrooms, and address,etc.

### Data Pipeline Diagram
![Pipeline Diagram](/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/flow_chart/dataflow git.gif)



### Visualization


### Technical Details

