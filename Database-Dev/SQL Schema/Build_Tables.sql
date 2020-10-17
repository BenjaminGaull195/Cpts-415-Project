CREATE TABLE TaxiZones (
    locationID INT NOT NULL,
    Borough VARCHAR NOT NULL,
    Zone VARCHAR NOT NULL,
    Service_Zone VARCHAR,
    PRIMARY KEY (locationID)
);

-- dispatching_base_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,SR_Flag
CREATE TABLE FHV_Trip_Records (
    Dispatch_base_num VARCHAR,
    Pickup_datetime VARCHAR NOT NULL,
    Dropoff_datetime VARCHAR NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    SR_Flag VARCHAR,
    PRIMARY KEY (PULocationID, DOLocationID, Pickup_datetime, Dropoff_datetime)
);

-- hvfhs_license_num,dispatching_base_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,SR_Flag
CREATE TABLE HV_FHV_Trip_Records (
    Hvfhs_license_num VARCHAR,
    Dispatch_base_num VARCHAR,
    Pickup_datetime VARCHAR NOT NULL,
    Dropoff_datetime VARCHAR NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    SR_Flag VARCHAR,
    PRIMARY KEY (PULocationID, DOLocationID, Pickup_datetime, Dropoff_datetime)
);

-- VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
CREATE TABLE Yellow_Cab_Trip_Records (
    VendorID INT,
    tpep_pickup_datetime VARCHAR NOT NULL,
    tpep_dropoff_datetime VARCHAR NOT NULL,
    Passenger_Count INT,
    Trip_Distance FLOAT,
    RateCodeID INT,
    Store_and_fwd_flag VARCHAR,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    Payment_type VARCHAR,
    Fare_ammount FLOAT,
    Extra VARCHAR,
    MTA_tax FLOAT,
    Tip_Amount FLOAT,
    Tolls_amount FLOAT,
    Improvement_surcharge FLOAT,
    Total_amount FLOAT,
    congestion_surcharge FLOAT,
    PRIMARY KEY (PULocationID, DOLocationID, tpep_pickup_datetime, tpep_dropoff_datetime)
);

-- VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge
CREATE TABLE Green_Cab_Trip_Records (
    VendorID INT,
    tpep_pickup_datetime VARCHAR NOT NULL,
    tpep_dropoff_datetime VARCHAR NOT NULL,
    Store_and_fwd_flag VARCHAR,
    RateCodeID INT,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    Passenger_Count INT,
    Trip_didstance FLOAT,    
    Extra VARCHAR,
    Fare_Amount FLOAT,
    MTA_tax FLOAT,
    Tip_amount FLOAT,
    Tolls_amount FLOAT,
    ehail_fee FLOAT,
    Improvement_surcharge FLOAT,
    Total_amount FLOAT,
    Payment_Type VARCHAR,
    Trip_type VARCHAR,
    congestion_surcharge FLOAT,
    PRIMARY KEY (PULocationID, DOLocationID, tpep_pickup_datetime, tpep_dropoff_datetime)
);
