import requests
import pandas as pd

def extract_zillow_sale_data(**kwargs):
        price_list = []
        for x in range(0,1000000,1000000):
                price_range = f"{x},{x+1000000}"
                price_list.append(price_range)
        for x in range(1000000,1000001,1):
                price_range = f"{x},"
                price_list.append(price_range)

        for i,price in enumerate(price_list):
                url=kwargs['url']
                headers=kwargs['headers']
                querystring=kwargs['querystring']
                querystring['priceRange'] = price
                file_suffix=f"zillow_sale_{i}"
                csv_file = f"/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/{file_suffix}.csv"
                response=requests.get(url,headers=headers, params=querystring)
                response_data=response.json()["data"]
                zpids = []
                latitudes = []
                longitudes = []
                addresses = []
                zipcodes = []
                cities=[]
                bedrooms=[]
                bathrooms=[]
                livingArea=[]
                yearBuilt=[]
                lot_size=[]
                lot_size_unit=[]
                propertyType=[]
                listingStatus=[]
                price=[]
                pricePerSquareFoot=[]
                zestimate=[]

                for element in response_data:
                        try:
                                zpids.append(element['zpid'])
                        except KeyError:
                                zpids.append(None)
                        try:
                                latitudes.append(element['location']['latitude'])
                        except KeyError:
                                latitudes.append(None)
                        try:
                                longitudes.append(element['location']['longitude'])
                        except KeyError:
                                longitudes.append(None)
                        try:
                                addresses.append(element['address']['streetAddress'])
                        except KeyError:
                                addresses.append(None)
                        try:
                                zipcodes.append(element['address']['zipcode'])
                        except KeyError:
                                zipcodes.append(None)
                        try:
                                cities.append(element['address']['city'])
                        except KeyError:
                                cities.append(None)
                        try:
                                bedrooms.append(element['bedrooms'])
                        except KeyError:
                                bedrooms.append(None)
                        try:
                                bathrooms.append(element['bathrooms'])
                        except KeyError:
                                bathrooms.append(None)
                        try:
                                livingArea.append(element['livingArea'])
                        except KeyError:
                                livingArea.append(None)
                        try:
                                yearBuilt.append(element['yearBuilt'])
                        except KeyError:
                                yearBuilt.append(None)
                        try:
                                lot_size.append(element['lotSizeWithUnit']['lotSize'])
                        except KeyError:
                                lot_size.append(None)
                        try:
                                lot_size_unit.append(element['lotSizeWithUnit']['lotSizeUnit'])
                        except KeyError:
                                lot_size_unit.append(None)
                        try:
                                propertyType.append(element['propertyType'])
                        except KeyError:
                                propertyType.append(None)
                        try:
                                listingStatus.append(element['listing']['listingStatus'])
                        except KeyError:
                                listingStatus.append(None)
                        try:
                                price.append(element['price']['value'])
                        except KeyError:
                                price.append(None)
                        try:
                                pricePerSquareFoot.append(element['price']['pricePerSquareFoot'])
                        except KeyError:
                                pricePerSquareFoot.append(None)
                        try:
                                zestimate.append(element['estimates']['zestimate'])
                        except KeyError:
                                zestimate.append(None)
                        

                df = pd.DataFrame({
                'zpid': zpids,
                'latitude': latitudes,
                'longitude': longitudes,
                'address': addresses,
                'zipcode': zipcodes,
                'cities':cities,
                'bedrooms':bedrooms,
                'bathrooms':bathrooms,
                'livingArea':livingArea,
                'yearBuilt':yearBuilt,
                'lot_size':lot_size,
                'lot_size_unit':lot_size_unit,
                'propertyType':propertyType,
                'listingStatus':listingStatus,
                'price':price,
                'pricePerSquareFoot':pricePerSquareFoot,
                'zestimate':zestimate

                })
                df.to_csv(csv_file)


def extract_zillow_sold_data(**kwargs):
        price_list = []
        for x in range(0,300000,300000):
                price_range = f"{x},{x+300000}"
                price_list.append(price_range)
        for x in range(300000,600000,20000):
                price_range = f"{x},{x+20000}"
                price_list.append(price_range)
        for x in range(600000,1200000,10000):
                price_range = f"{x},{x+10000}"
                price_list.append(price_range)
        for x in range(1200000,1300000,20000):
                price_range = f"{x},{x+20000}"
                price_list.append(price_range)
        for x in range(1300000,1600000,50000):
                price_range = f"{x},{x+50000}"
                price_list.append(price_range)
        for x in range(1600000,2000000,100000):
                price_range = f"{x},{x+100000}"
                price_list.append(price_range)
        for x in range (2000000, 2200000,200000):
                price_range = f"{x},{x+200000}"
                price_list.append(price_range)
        for x in range (2200000, 2500000,300000):
                price_range = f"{x},{x+300000}"
                price_list.append(price_range)
        for x in range (2500000, 3000000,500000):
                price_range = f"{x},{x+500000}"
                price_list.append(price_range)
        for x in range (3000000, 4000000,1000000):
                price_range = f"{x},{x+1000000}"
                price_list.append(price_range)
        for x in range (4000000, 4000001,1):
                price_range = f"{x},"
                price_list.append(price_range)



        for i,price in enumerate(price_list):
                url=kwargs['url']
                headers=kwargs['headers']
                querystring=kwargs['querystring']
                querystring['priceRange'] = price
                file_suffix=f"zillow_sold_{i}"
                csv_file = f"/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/{file_suffix}.csv"
                response=requests.get(url,headers=headers, params=querystring)
                response_data=response.json()["data"]
                zpids = []
                latitudes = []
                longitudes = []
                addresses = []
                zipcodes = []
                cities=[]
                bedrooms=[]
                bathrooms=[]
                livingArea=[]
                yearBuilt=[]
                lot_size=[]
                lot_size_unit=[]
                propertyType=[]
                listingStatus=[]
                price=[]
                pricePerSquareFoot=[]
                zestimate=[]
                for element in response_data:
                        try:
                                zpids.append(element['zpid'])
                        except KeyError:
                                zpids.append(None)
                        try:
                                latitudes.append(element['location']['latitude'])
                        except KeyError:
                                latitudes.append(None)
                        try:
                                longitudes.append(element['location']['longitude'])
                        except KeyError:
                                longitudes.append(None)
                        try:
                                addresses.append(element['address']['streetAddress'])
                        except KeyError:
                                addresses.append(None)
                        try:
                                zipcodes.append(element['address']['zipcode'])
                        except KeyError:
                                zipcodes.append(None)
                        try:
                                cities.append(element['address']['city'])
                        except KeyError:
                                cities.append(None)
                        try:
                                bedrooms.append(element['bedrooms'])
                        except KeyError:
                                bedrooms.append(None)
                        try:
                                bathrooms.append(element['bathrooms'])
                        except KeyError:
                                bathrooms.append(None)
                        try:
                                livingArea.append(element['livingArea'])
                        except KeyError:
                                livingArea.append(None)
                        try:
                                yearBuilt.append(element['yearBuilt'])
                        except KeyError:
                                yearBuilt.append(None)
                        try:
                                lot_size.append(element['lotSizeWithUnit']['lotSize'])
                        except KeyError:
                                lot_size.append(None)
                        try:
                                lot_size_unit.append(element['lotSizeWithUnit']['lotSizeUnit'])
                        except KeyError:
                                lot_size_unit.append(None)
                        try:
                                propertyType.append(element['propertyType'])
                        except KeyError:
                                propertyType.append(None)
                        try:
                                listingStatus.append(element['listing']['listingStatus'])
                        except KeyError:
                                listingStatus.append(None)
                        try:
                                price.append(element['price']['value'])
                        except KeyError:
                                price.append(None)
                        try:
                                pricePerSquareFoot.append(element['price']['pricePerSquareFoot'])
                        except KeyError:
                                pricePerSquareFoot.append(None)
                        try:
                                zestimate.append(element['estimates']['zestimate'])
                        except KeyError:
                                zestimate.append(None)

                df = pd.DataFrame({
                'zpid': zpids,
                'latitude': latitudes,
                'longitude': longitudes,
                'address': addresses,
                'zipcode': zipcodes,
                'cities':cities,
                'bedrooms':bedrooms,
                'bathrooms':bathrooms,
                'livingArea':livingArea,
                'yearBuilt':yearBuilt,
                'lot_size':lot_size,
                'lot_size_unit':lot_size_unit,
                'propertyType':propertyType,
                'listingStatus':listingStatus,
                'price':price,
                'pricePerSquareFoot':pricePerSquareFoot,
                'zestimate':zestimate
                })
                df.to_csv(csv_file)




def extract_zillow_rent_data(**kwargs):
        price_list = []
        for x in range(0,2000,2000):
                price_range = f"{x},{x+2000}"
                price_list.append(price_range)
        for x in range(2000,5000,500):
                price_range = f"{x},{x+500}"
                price_list.append(price_range)
        for x in range(5000,5001,1):
                price_range = f"{x},"
                price_list.append(price_range)



        for i,price in enumerate(price_list):
                url=kwargs['url']
                headers=kwargs['headers']
                querystring=kwargs['querystring']
                querystring['priceRange'] = price
                file_suffix=f"zillow_rent_{i}"
                csv_file = f"/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/{file_suffix}.csv"
                response=requests.get(url,headers=headers, params=querystring)
                response_data=response.json()["data"]
                zpids = []
                latitudes = []
                longitudes = []
                addresses = []
                zipcodes = []
                cities=[]
                bedrooms=[]
                bathrooms=[]
                livingArea=[]
                yearBuilt=[]
                lot_size=[]
                lot_size_unit=[]
                propertyType=[]
                listingStatus=[]
                price=[]
                pricePerSquareFoot=[]
                zestimate=[]
                for element in response_data:
                        try:
                                zpids.append(element['zpid'])
                        except KeyError:
                                zpids.append(None)
                        try:
                                latitudes.append(element['location']['latitude'])
                        except KeyError:
                                latitudes.append(None)
                        try:
                                longitudes.append(element['location']['longitude'])
                        except KeyError:
                                longitudes.append(None)
                        try:
                                addresses.append(element['address']['streetAddress'])
                        except KeyError:
                                addresses.append(None)
                        try:
                                zipcodes.append(element['address']['zipcode'])
                        except KeyError:
                                zipcodes.append(None)
                        try:
                                cities.append(element['address']['city'])
                        except KeyError:
                                cities.append(None)
                        try:
                                bedrooms.append(element['bedrooms'])
                        except KeyError:
                                bedrooms.append(None)
                        try:
                                bathrooms.append(element['bathrooms'])
                        except KeyError:
                                bathrooms.append(None)
                        try:
                                livingArea.append(element['livingArea'])
                        except KeyError:
                                livingArea.append(None)
                        try:
                                yearBuilt.append(element['yearBuilt'])
                        except KeyError:
                                yearBuilt.append(None)
                        try:
                                lot_size.append(element['lotSizeWithUnit']['lotSize'])
                        except KeyError:
                                lot_size.append(None)
                        try:
                                lot_size_unit.append(element['lotSizeWithUnit']['lotSizeUnit'])
                        except KeyError:
                                lot_size_unit.append(None)
                        try:
                                propertyType.append(element['propertyType'])
                        except KeyError:
                                propertyType.append(None)
                        try:
                                listingStatus.append(element['listing']['listingStatus'])
                        except KeyError:
                                listingStatus.append(None)
                        try:
                                price.append(element['price']['value'])
                        except KeyError:
                                price.append(None)
                        try:
                                pricePerSquareFoot.append(element['price']['pricePerSquareFoot'])
                        except KeyError:
                                pricePerSquareFoot.append(None)
                        try:
                                zestimate.append(element['estimates']['zestimate'])
                        except KeyError:
                                zestimate.append(None)

                df = pd.DataFrame({
                'zpid': zpids,
                'latitude': latitudes,
                'longitude': longitudes,
                'address': addresses,
                'zipcode': zipcodes,
                'cities':cities,
                'bedrooms':bedrooms,
                'bathrooms':bathrooms,
                'livingArea':livingArea,
                'yearBuilt':yearBuilt,
                'lot_size':lot_size,
                'lot_size_unit':lot_size_unit,
                'propertyType':propertyType,
                'listingStatus':listingStatus,
                'price':price,
                'pricePerSquareFoot':pricePerSquareFoot,
                'zestimate':zestimate
                })
                df.to_csv(csv_file)