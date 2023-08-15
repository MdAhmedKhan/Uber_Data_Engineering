if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
@custom
def transform_custom(df, *args, **kwargs):
    """
    Args:
        df: The output from the upstream parent block (if applicable)
        args: The output from any additional upstream blocks

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here



   
    df=df.drop_duplicates().reset_index(drop=True)
    df['trip_id']=df.index

    df['tpep_pickup_datetime']= pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])

    Datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    Datetime_dim['datetime_id']=Datetime_dim.index
    Datetime_dim['pickup_hour']=Datetime_dim['tpep_pickup_datetime'].dt.hour
    Datetime_dim['pickup_day']=Datetime_dim['tpep_pickup_datetime'].dt.day
    Datetime_dim['pickup_month']=Datetime_dim['tpep_pickup_datetime'].dt.month
    Datetime_dim['pickup_year']=Datetime_dim['tpep_pickup_datetime'].dt.year
    Datetime_dim['pickup_weekday']=Datetime_dim['tpep_pickup_datetime'].dt.weekday
    Datetime_dim['drop_hour']=Datetime_dim['tpep_dropoff_datetime'].dt.hour
    Datetime_dim['drop_day']=Datetime_dim['tpep_dropoff_datetime'].dt.day
    Datetime_dim['drop_month']=Datetime_dim['tpep_dropoff_datetime'].dt.month
    Datetime_dim['drop_year']=Datetime_dim['tpep_dropoff_datetime'].dt.year
    Datetime_dim['drop_weekday']=Datetime_dim['tpep_dropoff_datetime'].dt.weekday

    Datetime_dim = Datetime_dim[['datetime_id','tpep_pickup_datetime','tpep_dropoff_datetime','pickup_hour','pickup_day','pickup_month','pickup_year','pickup_weekday','drop_hour','drop_day','drop_month','drop_year','drop_weekday']]

    Passenger_Count_Dim= df[['passenger_count']].reset_index(drop=True)
    Passenger_Count_Dim['pass_count_Id']= Passenger_Count_Dim.index
    Passenger_Count_Dim= Passenger_Count_Dim[['pass_count_Id','passenger_count']]

    drop_location_dim= df[['dropoff_longitude','dropoff_latitude']].reset_index(drop=True)
    drop_location_dim['drop_location_id']= drop_location_dim.index
    drop_location_dim= drop_location_dim[['drop_location_id','dropoff_longitude','dropoff_latitude']]

    payment_name = {
    1: "Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip" }

    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id']=payment_type_dim.index
    payment_type_dim['payment_type']=df[['payment_type']]
    payment_type_dim['payment_name']=payment_type_dim['payment_type'].map(payment_name)
    payment_type_dim= payment_type_dim[['payment_type_id','payment_type','payment_name']]

    rate_code_name = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"}

    rate_code_dim= df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id']=rate_code_dim.index
    rate_code_dim['rate_code']=df[['RatecodeID']]
    rate_code_dim['rate_code_name']=rate_code_dim['rate_code'].map(rate_code_name)
    rate_code_dim=rate_code_dim[['rate_code_id','rate_code','rate_code_name']]

    Trip_Distance_Dim = df[['trip_distance']].reset_index(drop=True)
    Trip_Distance_Dim['trip_distance_Id'] = Trip_Distance_Dim.index
    Trip_Distance_Dim['trip_distance']=df[['trip_distance']]
    Trip_Distance_Dim=Trip_Distance_Dim[['trip_distance_Id','trip_distance']]

    pickup_location_dim= df[['pickup_longitude','pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id']= pickup_location_dim.index
    pickup_location_dim= pickup_location_dim[['pickup_location_id','pickup_longitude','pickup_latitude']]

    fact_table = [['Trip_id','Vendor_ID','Datetime_id','trip_distance_Id','rate_code_id','pass_count_Id','pickup_location_id','drop_location_id','Payment_type_id','store_and_fwd_flag','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']]

    fact_table= df.merge(Datetime_dim,left_on='trip_id',right_on='datetime_id') \
    .merge(Passenger_Count_Dim,left_on='trip_id',right_on='pass_count_Id') \
    .merge(drop_location_dim,left_on='trip_id',right_on='drop_location_id') \
    .merge(Trip_Distance_Dim,left_on='trip_id',right_on='trip_distance_Id') \
    .merge(payment_type_dim,left_on='trip_id',right_on='payment_type_id') \
    .merge(rate_code_dim,left_on='trip_id',right_on='rate_code_id') \
    .merge(pickup_location_dim,left_on='trip_id',right_on='pickup_location_id') \
    [['trip_id','VendorID','datetime_id','rate_code_id','trip_distance_Id','pass_count_Id','pickup_location_id',
    'drop_location_id','payment_type_id','store_and_fwd_flag','fare_amount','extra','mta_tax',
    'tip_amount','tolls_amount','improvement_surcharge','total_amount']]

    
    return{"datetime_dim":Datetime_dim.to_dict(orient="dict"),
    "passenger_count_dim":Passenger_Count_Dim.to_dict(orient="dict"),
    "trip_distance_dim":Trip_Distance_Dim.to_dict(orient="dict"),
    "rate_code_dim":rate_code_dim.to_dict(orient="dict"),
    "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
    "dropoff_location_dim":drop_location_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
