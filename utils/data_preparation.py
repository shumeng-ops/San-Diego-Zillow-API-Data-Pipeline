import pandas as pd
import os
import boto3

def combine_data(**kwargs):
    directory=kwargs['directory']
    combined_file=kwargs['combined_file']
    csv_files =[file for file in os.listdir(directory) if file.endswith('.csv')]
    dfs = []
    for file in csv_files:
        file_path = os.path.join(directory, file)
        df = pd.read_csv(file_path)
        df['source']=file
        dfs.append(df)

    # Concatenate all DataFrames in the list into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    # Write the combined DataFrame to a CSV file
    combined_df.to_csv(combined_file, index=False)


def remove_duplicate(**kwargs):
    raw_file=kwargs['raw_file']
    dedupe_file=kwargs['dedupe_file']
    raw_data=pd.read_csv(raw_file)
    print(f"Rows before deduplication: {raw_data.shape[0]}")
    raw_data =raw_data.sort_values(by='listingStatus', ascending=False)
    dedup_data = raw_data.drop_duplicates(subset=['zpid'])
    print(f"Rows after deduplication: {dedup_data.shape[0]}")
    dedup_data.to_csv(dedupe_file, index=False)


def upload_to_s3(**kwargs):
    s3_client=kwargs['s3_client']
    file_directory=kwargs['file_directory']
    bucket_name=kwargs['bucket_name']
    key=kwargs['key']
    s3_client.upload_file(file_directory, bucket_name, key)


def zillow_cleanup(**kwargs):
    file_path=kwargs['file_path']
    output_path=kwargs['output_path']
    data=pd.read_csv(file_path)
    #drop the first column
    data.drop(columns=['Unnamed: 0'], inplace=True)
    data.drop(columns=['source'], inplace=True)
    #Filter out Land and Manufactured
    data = data[data['propertyType'].isin(['condo','apartment','townhome','singleFamily','multiFamily'])]
    #Fill out NA
    columns_fill=['yearBuilt','bedrooms','bathrooms']
    data[columns_fill] = data[columns_fill].fillna(0)
    ## Change data type
    data['zpid'] = data['zpid'].astype('str')
    data['zipcode'] = data['zipcode'].astype('str')
    data['bedrooms'] = data['bedrooms'].astype('int')
    data['yearBuilt'] = data['yearBuilt'].astype('int')
    #Remove Records with invalid living Area
    data = data[~data['livingArea'].isnull()]
    data = data[~(data['livingArea'] == 0)]
    #Create new columns to combine price and zestimate
    data['estimated_price'] = data.apply(lambda row: row['price'] if row['listingStatus'] == 'forSale' \
                                     else row['zestimate'], axis=1)
    #Remove records that doesn't have total price
    data = data[~data['estimated_price'].isnull()]
    #Create new columns of price_sqft
    data['estimated_price_sqft'] = data['estimated_price'] / data['livingArea']
    data.to_csv(output_path, index=False)


