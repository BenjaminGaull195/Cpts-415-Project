using System;
//commonly used libraries
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;

//Spark libraries
using Microsoft.Spark;
using Microsoft.Spark.Sql;


namespace Taxi_Spark_Linux
{
    class Program
    {
        static void Main(string[] args)
        {
            //Initialize Spark SQL session 
            SparkSession spark = SparkSession.Builder().AppName("Taxi-Spark").GetOrCreate();


            //Build Dataset; TODO: Test reading multiple files from directory
            //Dataframes contain the data from the months of January - July.
            DataFrame YellowTaxi_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/yellow/");
            DataFrame GreenTaxi_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/green/");
            DataFrame Fhv_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/fhv/");
            DataFrame Hvfhv_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/fhvhv/");
            DataFrame Zonedata_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zonesXY.csv");

            //Unioned the above dataframes with temporary ones containing the rest of the data

            
            //Test Lines
            Console.WriteLine("Debug:: Dataframe Schems:");
            GreenTaxi_dataFrame.PrintSchema();
            YellowTaxi_dataFrame.PrintSchema();
            Fhv_dataFrame.PrintSchema();
            Hvfhv_dataFrame.PrintSchema();
            Zonedata_dataFrame.PrintSchema();

            Console.WriteLine("Debug:: Dataframes:");
            GreenTaxi_dataFrame.Show();
            YellowTaxi_dataFrame.Show();
            Fhv_dataFrame.Show();
            Hvfhv_dataFrame.Show();
            Zonedata_dataFrame.Show();



            //Initial queries to aquire necessary data
            long num_taxi_zones = getNumTaxiZones(ref Zonedata_dataFrame);
            Console.WriteLine(num_taxi_zones.ToString());
        }

        //Queries:

        /// <summary>
        /// Used to get the total number of taxizones
        /// </summary>
        /// <param name="Taxi_Zones">pass by ref variable refering to taxi zone dataframe</param>
        /// <returns>num taxi zones</returns>
        public static long getNumTaxiZones(ref DataFrame Taxi_Zones)
        {
            return Taxi_Zones.Count();
        }


        ///// <summary>
        ///// Returns a list of neighbors for the current taxi zone
        ///// </summary>
        ///// <param name="Taxi_Zones">pass by ref variable refering to taxi zone dataframe</param>
        ///// <param name="currentZoneID">ID for the current taxi zone</param>
        ///// <returns></returns>
        //public static List<NeighborData> GetNeighbors(ref DataFrame Taxi_Zones, int currentZoneID)
        //{

        //}

        ///// <summary>
        ///// Returns the total number of yellow taxi trips that have a pickup/dropoff that match the currentZoneID
        ///// </summary>
        ///// <param name="Yellow_Tripdata">pass by ref variable refering to yellow_tripdata dataframe</param>
        ///// <param name="currentZoneID">ID for the current taxi zone</param>
        ///// <returns></returns>
        //public static int getNumYellowTrips(ref DataFrame Yellow_Tripdata, int currentZoneID)
        //{
        //Yellow_Tripdata.Filter("PULocationID = 'currentZoneID' OR DOLocationID = 'currentZoneID'").Count();
        //}

        ///// <summary>
        ///// Returns the total number of green taxi trips that have a pickup/dropoff that match the currentZoneID
        ///// </summary>
        ///// <param name="Green_Tripdata">pass by ref variable refering to green_tripdata dataframe</param>
        ///// <param name="currentZoneID">ID for the current taxi zone</param>
        ///// <returns></returns>
        //public static int getNumGreenTrips(ref DataFrame Green_Tripdata, int currentZoneID)
        //{
        //Green_Tripdata.Filter("PULocationID = 'currentZoneID' OR DOLocationID = 'currentZoneID'").Count();
        //}

        ///// <summary>
        ///// Returns the total number of fhv trips that have a pickup/dropoff that match the currentZoneID
        ///// </summary>
        ///// <param name="FHV_Tripdata">pass by ref variable refering to fhv_tripdata dataframe</param>
        ///// <param name="currentZoneID">ID for the current taxi zone</param>
        ///// <returns></returns>
        //public static int getNumFHVTrips(ref DataFrame FHV_Tripdata, int currentZoneID)
        //{
        //FHV_Tripdata.Filter("PULocationID = 'currentZoneID' OR DOLocationID = 'currentZoneID'").Count();
        //}

        ///// <summary>
        ///// Returns the total number of hvfhv trips that have a pickup/dropoff that match the currentZoneID
        ///// </summary>
        ///// <param name="HVFHV_Tripdata">pass by ref variable refering to hvfhv_tripdata dataframe</param>
        ///// <param name="currentZoneID">ID for the current taxi zone</param>
        ///// <returns></returns>
        //public static int getNumHVFHVTrips(ref DataFrame HVFHV_Tripdata, int currentZoneID)
        //{
        //HVFHV_Tripdata.Filter("PULocationID = 'currentZoneID' OR DOLocationID = 'currentZoneID'").Count();
        //}




    }
}
