# This is a sample Python script.
#
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time

def AddQuotes(string):
    quote = "'"
    return quote + string + quote


def add_comma_tail(string):
    return string + ','


def Parse_Taxi_Zones(path,output):
    f = open(path, "r")
    #o = open(output, "w")
    locid = -1
    borough = ''
    zone = ''
    service_zone = -1
    i = 0
    for line in f:
        if i == 0:
            line = line.strip()
            words = line.split(',')
            locid = words[0]
            borough = AddQuotes(words[1])
            zone = AddQuotes(words[2])
            service_zone = AddQuotes(words[3])
           # o.write('INSERT INTO TaxiZones VALUES (' + locid + ',' + borough + ',' + zone + ',' + service_zone + ')'+'\n')
            print('INSERT INTO TaxiZones VALUES (' + locid + ',' + borough + ',' + zone + ',' + service_zone + ')'+'\n')
        i = 1
    f.close()
    #o.close()

def Parse_Yellow_Cab_Records(input,output):
    f = open(input, "r")
    o = open(output, "w")
    i = 0
    pu_loc_id, do_loc_id = 0, 0
    tpep_pickup_date, tpep_dropoff_date = '', ''
    total_amount = 0.0
    ratecode_id = 0
    trip_distance = 0.0
    vendor_id = 0
    store_and_fwd_flag, payment_type = '', ''
    fare_amount = 0.0
    extra = ''
    mta_tax, improv_surcharge = 0.0, 0.0
    tip_amount, tolls_amount = 0.0, 0.0
    for line in f:
        if i == 0:
            line = line.strip()
            words = line.split(',')
            for item in words:
                item = add_comma_tail(item)
            words[16] = words[16].strip()

            pu_loc_id, do_loc_id = words[0], words[1]
            tpep_pickup_date, tpep_dropoff_date = AddQuotes(words[2]), AddQuotes(words[3])
            total_amount, ratecode_id = words[4], words[5]
            trip_distance, vendor_id = words[6], words[7]
            store_and_fwd_flag, payment_type = AddQuotes(words[8]), AddQuotes(words[9])
            fare_amount, fare_amount = words[10], words[11]
            extra, mta_tax = AddQuotes(words[12]), words[13]
            improv_surcharge, tip_amount, tolls_amount = words[14], words[15], words[16]
            o.write('INSERT INTO TaxiZones VALUES (' + pu_loc_id + do_loc_id + tpep_pickup_date + tpep_dropoff_date + total_amount + ratecode_id + trip_distance + vendor_id + store_and_fwd_flag + payment_type + fare_amount + extra + mta_tax + improv_surcharge + tip_amount + tolls_amount + ')' + '\n')
        i = 1

#Need to test writing to files.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = time.time()
    Parse_Taxi_Zones(r'C:\Users\Kendrick\Documents\5th Year\Big Data\Data\Taxiszones.csv', '')
    end = time.time()
    start = end - start
    print('seconds elapsed are', start)

    start = time.time()
    #Parse_Yellow_Cab_Records('', '')
    end = time.time()
    start = end - start
    print('seconds elapsed are', start)

# Assuming the tables are already made.
# parse the csv files line by line to receive values
# INSERT INTO table_name VALUES (value1, value2, value3, ...); <-Order matters in this case
