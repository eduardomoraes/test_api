import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from fastapi import FastAPI, Request
from azure.storage.filedatalake import DataLakeServiceClient
import pyarrow.parquet as pq
import io, os, uuid, sys
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

app = FastAPI()

# load the dataset
service_client = DataLakeServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=az1danasdl003;AccountKey=IpqYRWCUizcU5KqZ1kZExdgrlfZt5nbqJEBXz92mN/yGE8Rs3Bllctj/xWDTfAP5pt2ZOtTjn6BV+ASt4aPtnQ==;EndpointSuffix=core.windows.net')
file_system_client = service_client.get_file_system_client(file_system = 'knime')
directory_client = file_system_client.get_directory_client('/test/')
file_client = directory_client.get_file_client('model_rec_dataset_2.parquet')
download = file_client.download_file()
downloaded_bytes = download.readall()
output_table = pq.read_table(io.BytesIO(downloaded_bytes))
input_table_1 = output_table.to_pandas()
input_table_1.columns = input_table_1.columns.astype(str)

# preprocess the data
one_hot_encoder = OneHotEncoder()
cols = ['Varietal', 'Unit Size', 'Package Type', 'Sugar Content g/L Range', 'Sub Brand', 'Flavour', 'Consolidated Colour', 'Brand Family']
if 'customer' in cols:
    cols.remove('customer')
input_table_1 = input_table_1.reset_index(drop=True)
input_table_2 = pd.DataFrame(one_hot_encoder.fit_transform(input_table_1[cols]).toarray())
input_table_3 = input_table_2.reset_index(drop=True)
input_table_4 = pd.concat([input_table_1['product'], input_table_3], axis=1)


# fit the model
model = NearestNeighbors(n_neighbors=11, metric='cosine')
model.fit(input_table_4.drop(columns=['product']))


# get recommendations
def get_recommendations(product, model, dataset, n=10):
    product_index = dataset[dataset['product'] == product].index[0]
    distances, indices = model.kneighbors(dataset.drop(columns=['product']).loc[product_index].values.reshape(1, -1), n_neighbors=n+1)
    recommendations = pd.DataFrame({'product': dataset.iloc[indices[0][1:]]['product'].values,
                                    'distance': distances[0][1:]})
    return recommendations

# define the endpoint
@app.get("/recommendations/")
def recommendations(product: str):
    recommendations = get_recommendations(product, model, input_table_4)
    #return recommendations.to_dict()
    return {"recommendations": recommendations.to_dict()}


