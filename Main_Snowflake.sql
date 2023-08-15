CREATE VIEW uber_view (trip_id,
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code_name,
        pickup_latitude,
        pickup_longitude,
        dropoff_latitude,
        pickup_hour,
        dropoff_longitude,
        payment_name,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount)AS 

    SELECT
        f."trip_id",
        f."VendorID",
        d."tpep_pickup_datetime",
        d."tpep_dropoff_datetime",
        p."passenger_count",
        t."trip_distance",
        r."rate_code_name",
        pk."pickup_latitude",
        pk."pickup_longitude",
        dr."dropoff_latitude",
        d."pickup_hour",
        dr."dropoff_longitude",
        pay."payment_name",
        f."fare_amount",
        f."extra",
        f."mta_tax",
        f."tip_amount",
        f."tolls_amount",
        f."improvement_surcharge",
        f."total_amount"
    FROM 
        UBER.DATA_ANALYTICS."fact_table" f
    JOIN 
        UBER.DATA_ANALYTICS."datetime_dim" d ON f."datetime_id" = d."datetime_id"
    JOIN 
        UBER.DATA_ANALYTICS."passenger_count_dim" p ON p."pass_count_Id" = f."pass_count_Id"
    JOIN 
        UBER.DATA_ANALYTICS."trip_distance_dim" t ON t."trip_distance_Id" = f."trip_distance_Id"
    JOIN 
        UBER.DATA_ANALYTICS."rate_code_dim" r ON r."rate_code_id" = f."rate_code_id"
    JOIN 
        UBER.DATA_ANALYTICS."pickup_location_dim" pk ON pk."pickup_location_id" = f."pickup_location_id"
    JOIN 
        UBER.DATA_ANALYTICS."dropoff_location_dim" dr ON dr."drop_location_id" = f."drop_location_id"
    JOIN 
        UBER.DATA_ANALYTICS."payment_type_dim" pay ON pay."payment_type_id" = f."payment_type_id";


