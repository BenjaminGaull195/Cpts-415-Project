
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
using System.Linq;

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

            Stopwatch stopwatch = new Stopwatch();
            string elapsedTime = "";
            TimeSpan ts;

            //Build Dataset; TODO: Test reading multiple files from directory
            //Dataframes contain the data from the months of January - July.
            Console.WriteLine("Debug::");

            //
            stopwatch.Start();
            DataFrame YellowTaxi_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/yellow/");
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nyellow Taxi:");
            Console.WriteLine("Time to load Yellow Taxi Data: " + elapsedTime);
            Console.WriteLine("Number of entries: {0}", YellowTaxi_dataFrame.Count());
            YellowTaxi_dataFrame.PrintSchema();
            YellowTaxi_dataFrame.Show();
            

            //Green Taxi Data Load and Debug Info
            stopwatch.Start();
            DataFrame GreenTaxi_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/green/");
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nGreen Taxi:");
            Console.WriteLine("Time to load Green Taxi Data: " + elapsedTime);
            Console.WriteLine("Number of entries: {0}", GreenTaxi_dataFrame.Count());
            GreenTaxi_dataFrame.PrintSchema();
            GreenTaxi_dataFrame.Show();
            

            //FHV Data Load and Debug Info
            stopwatch.Start();
            DataFrame Fhv_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/fhv/");
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nFor Hire Vehicle:");
            Console.WriteLine("Time to load FHV Data: " + elapsedTime);
            Console.WriteLine("Number of entries: {0}", Fhv_dataFrame.Count());
            Fhv_dataFrame.PrintSchema();
            Fhv_dataFrame.Show();
            

            //HVFHV Data Load and Debug Info
            stopwatch.Start();
            DataFrame Hvfhv_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/fhvhv/");
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nHigh Volume For Hire Vehicle:");
            Console.WriteLine("Time to load FHVHV Data: " + elapsedTime);
            Console.WriteLine("Number of entries: {0}", Hvfhv_dataFrame.Count());
            Hvfhv_dataFrame.PrintSchema();
            Hvfhv_dataFrame.Show();
            

            //Taxi Zone Data Load and Debug Info
            stopwatch.Start();
            DataFrame Zonedata_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/TEST/taxi_zonesXY_test.csv");
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nTaxi Zones:");
            Console.WriteLine("Time to load Taxi Zone Data: " + elapsedTime);
            Console.WriteLine("Number of entries: {0}", Zonedata_dataFrame.Count());
            Zonedata_dataFrame.PrintSchema();
            Zonedata_dataFrame.Show();
            

            //If Neighbor Data does not exist, generate data
	        if(!File.Exists("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zoneNeighbors.csv")) 
            {
                Console.WriteLine("Could not find existing Neighbor Table data, building new file from Taxi Zone Data");
                stopwatch.Start();
	            //run BuildNeighborTable() function
                int numZones = (int)Zonedata_dataFrame.Count();
                BuildNeighborTable(numZones, 0.51,ref Zonedata_dataFrame);

                stopwatch.Stop();
                ts = stopwatch.Elapsed;
                elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
                Console.WriteLine("Time to build Taxi Zone Neighbor Data: " + elapsedTime);
            }

            //Neighbor Data Load and Debug Info
            stopwatch.Start();
            DataFrame NeighborTable = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zoneNeighbors.csv");
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nDebug:: Neighbor Table:");
            Console.WriteLine("Time to load Taxi Zone Neighbor Data: " + elapsedTime);
            Console.WriteLine("Number of entries: {0}", NeighborTable.Count());
            NeighborTable.PrintSchema();
            NeighborTable.Show();
            

            
            //Initial queries to aquire necessary data
            


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


        /// <summary>
        /// Returns a list of neighbors for the current taxi zone
        /// </summary>
        /// <param name="Taxi_Zones">pass by ref variable refering to taxi zone dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns></returns>
        //public static List<NeighborData> GetNeighbors(ref DataFrame Taxi_Zones, int currentZoneID)
        //{
        //
        //}

        /// <summary>
        /// Returns the total number of yellow taxi trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="Yellow_Tripdata">pass by ref variable refering to yellow_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns></returns>
        public static long getNumYellowTrips(ref DataFrame Yellow_Tripdata, int currentZoneID)
        {
            return Yellow_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
        }

        /// <summary>
        /// Returns the total number of green taxi trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="Green_Tripdata">pass by ref variable refering to green_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns></returns>
        public static long getNumGreenTrips(ref DataFrame Green_Tripdata, int currentZoneID)
        {
            return Green_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
        }

        /// <summary>
        /// Returns the total number of fhv trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="FHV_Tripdata">pass by ref variable refering to fhv_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns></returns>
        public static long getNumFHVTrips(ref DataFrame FHV_Tripdata, int currentZoneID)
        {
            return FHV_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
        }

        /// <summary>
        /// Returns the total number of hvfhv trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="HVFHV_Tripdata">pass by ref variable refering to hvfhv_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns></returns>
        public static long getNumHVFHVTrips(ref DataFrame HVFHV_Tripdata, int currentZoneID)
        {
            return HVFHV_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
        }





        public static double IsNeighbor(double radius, double x1, double y1, double x2, double y2)
        {
            x1 = Math.Pow((x1 - x2),2);
            y1 = Math.Pow((y1 - y2),2);
            if (radius >= Math.Sqrt((x1 + y1))){
                return Math.Sqrt((x1 + y1));
            }
            return -1;
        }	


	    private static void BuildNeighborTable(int num_zones, double radius, ref DataFrame Zone_Lookup)
        {//We require a table detailing a neighboring regions in the DB.
            //var path = @"file.csv";//<-Will need to change this for release.
            //string text = "100,200,1.4150000"; 
            //byte[] data = Encoding.ASCII.GetBytes(text); 
            double distance = 0.0;
        
            Row[] taxi_zones = Zone_Lookup.Head(num_zones).ToArray<Row>();//.ToArray();
            Console.WriteLine("Debug: taxi_zones size = {0}", taxi_zones.Length);
            //create two clones of Zone_Lookup
                                
            using (StreamWriter output = new StreamWriter("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zoneNeighbors.csv")) {
                output.WriteLine("currentZone,neighbor,distance");

                Console.WriteLine("Right before iteration");
                //Iteratively pull the coordinates from the taxi zone table. It only has num_zones items which should be 256 
                for (int i = 0; i < num_zones; ++i)
                {
                    //df3 = Zone_Lookup.Filter(String.Format("LocationID = \"{0}\"", i));
                    for (int j = 0; j < num_zones; ++j)
                    {
                        //df4 = Zone_Lookup.Filter(String.Format("LocationID = \"{0}\"", j));
                        distance = IsNeighbor(radius, Convert.ToDouble(taxi_zones[i].Get(4)), Convert.ToDouble(taxi_zones[i].Get(5)), Convert.ToDouble(taxi_zones[j].Get(4)), Convert.ToDouble(taxi_zones[j].Get(5)));

                        if (distance != -1 && i != j)
                        {
                            //text = i.ToString() + "," + j.ToString() + "," + distance.ToString() + "\n";
                            //data = Encoding.ASCII.GetBytes(text);
                            //File.WriteAllBytes(path, data);
                            //Write to file
                        
                            output.WriteLine("{0},{1},{2}", i.ToString(), j.ToString(), distance);
                            //Console.WriteLine("{0},{1},{2}",i.ToString(), j.ToString(),distance.ToString());
                        }
                    }
                    //In a nested loop compare coordinates with all other regions which you also pull from the taxi zone table
                }
            }
        }
    }
}
