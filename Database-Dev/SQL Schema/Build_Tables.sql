CREATE TABLE TaxiZones (
    locationID INT NOT NULL,
    Borough VARCHAR NOT NULL,
    Zone VARCHAR NOT NULL,
    Service_Zone VARCHAR,
    PRIMARY KEY (locationID)
);

CREATE TABLE FHV_Trip_Records (
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    Pickup_datetime VARCHAR NOT NULL,
    Dropoff_datetime VARCHAR NOT NULL,
    Dispatch_base_num INT,
    SR_Flag VARCHAR,
    PRIMARY KEY (PULocationID, DOLocationID, Pickup_datetime, Dropoff_datetime)
);

CREATE TABLE HV_FHV_Trip_Records (
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    Pickup_datetime VARCHAR NOT NULL,
    Dropoff_datetime VARCHAR NOT NULL,
    Dispatch_base_num INT,
    SR_Flag VARCHAR,
    Hvfhs_license_num VARCHAR,
    PRIMARY KEY (PULocationID, DOLocationID, Pickup_datetime, Dropoff_datetime)
);

CREATE TABLE Yellow_Cab_Trip_Records (
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    tpep_pickup_datetime VARCHAR NOT NULL,
    tpep_dropoff_datetime VARCHAR NOT NULL,
    Total_amount FLOAT,
    RateCodeID INT,
    Trip_Distance FLOAT,
    VendorID INT,
    Store_and_fwd_flag VARCHAR,
    Payment_type VARCHAR,
    Fare_ammount FLOAT,
    Extra VARCHAR,
    MTA_tax FLOAT,
    Improvement_surcharge FLOAT,
    Tip_Amount FLOAT,
    Tolls_amount FLOAT,
    PRIMARY KEY (PULocationID, DOLocationID, tpep_pickup_datetime, tpep_dropoff_datetime)
);

CREATE TABLE Green_Cab_Trip_Records (
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    tpep_pickup_datetime VARCHAR NOT NULL,
    tpep_dropoff_datetime VARCHAR NOT NULL,
    Total_amount FLOAT,
    RateCodeID INT,
    Trip_didstance FLOAT,
    VendorID INT,
    Store_and_fwd_flag VARCHAR,
    Payment_Type VARCHAR,
    Fare_Amount FLOAT,
    Extra VARCHAR,
    MTA_tax FLOAT,
    Improvement_surcharge FLOAT,
    Tip_amount FLOAT,
    Tolls_amount FLOAT,
    Trip_type VARCHAR,
    PRIMARY KEY (PULocationID, DOLocationID, tpep_pickup_datetime, tpep_dropoff_datetime)
);
