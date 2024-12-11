from fastapi import FastAPI
from pydantic import BaseModel, ValidationError, EmailStr, Field
import uvicorn  
# import joblib
import boto3
import pandas as pd
from io import BytesIO
import pyarrow as pa
import os
from typing import List, Optional
from fastapi.responses import JSONResponse

app = FastAPI()

def read_latest_parquet_from_s3(bucket_name, prefix):
    try:
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if not aws_access_key_id or not aws_secret_access_key:
            raise EnvironmentError("AWS credentials not found in environment variables.")

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        all_files = response.get('Contents', [])
        if not all_files:
            raise FileNotFoundError("No files found in the specified S3 bucket and prefix.")
        
        latest_file = max(all_files, key=lambda x: x['LastModified'])
        latest_file_key = latest_file['Key']
        print(f"Latest file key: {latest_file_key}")
        
        obj = s3.get_object(Bucket=bucket_name, Key=latest_file_key) 
        file_content = obj['Body'].read()

        if len(file_content) == 0:
            raise ValueError("The Parquet file is empty.")

        df = pd.read_parquet(BytesIO(file_content)) 

        return df
    except Exception as e:
        print(f"Error in read_latest_parquet_from_s3: {e}")
        return None





class taxitrips_model(BaseModel):
    VendorID: int
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    request_datetime: str
    passenger_count: int
    trip_distance: Optional[float]
    RatecodeID: int
    store_and_fwd_flag: str
    PULocationID: int
    DOLocationID: int
    payment_type: int
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    total_amount: float
    congestion_surcharge: float
    airport_fee: float
    driver_email: str
    driver_phone_number: str
    driver_fullname: str
    driver_credit_card: int
    passenger_email: str
    passenger_phone_number: str
    passenger_fullname: str
    passenger_credit_card: int 
    passenger_address: str = Field(..., min_length=3, max_length=150)
    passenger_Job: str
    passenger_age: int = Field(..., gt=0)
    passenger_sex: str
    pickup_latitude: float
    pickup_longitude: float
    dropoff_latitude: float
    dropoff_longitude: float
    pickup_AQI: int
    dropoff_AQI: int
    temperature: float
    humidity: float
    pickup_precipitation_chance: float
    uv_index: int
    feels_like: float
    weather_description: str
    wind_speed_km: float

@app.get("/", response_model=taxitrips_model)
async def root():
    # read the latest parquet file from S3
    df = read_latest_parquet_from_s3("databricks-workspace-stack-e7e6f-bucket", "unity-catalog/taxi-data-dev_silver")
    
    # Convert DataFrame to a list of dictionaries
    result = df.to_dict(orient="records")

    # validate 
    validated_results = [taxitrips_model(**record) for record in result]
    return JSONResponse(content=[record.dict() for record in validated_results])

@app.get("/filter_by_date", response_model=List[taxitrips_model])
async def filter_by_date(request_datetime: str):
    # read the latest parquet file from S3
    df = read_latest_parquet_from_s3("databricks-workspace-stack-e7e6f-bucket", "unity-catalog/taxi-data-dev_silver")
    
    # Filter DataFrame by request_datetime
    filtered_df = df[df['request_datetime'] == request_datetime]
    
    # Convert DataFrame to a list of dictionaries
    result = filtered_df.to_dict(orient="records")

    # Validate and return the filtered results
    validated_results = [taxitrips_model(**record) for record in result]
    return JSONResponse(content=[record.dict() for record in validated_results])

@app.post("/add_trip", response_model=taxitrips_model)
async def add_trip(trip: taxitrips_model):
    # sample dev >> TODO

    try:
        df = trip.dict()
        res = taxitrips_model(**df)    

    except ValidationError as e:
        print(e.json())

    return JSONResponse(content=trip.dict())
    
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
