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
using Algorithms;

//Formatted/Semi-structured IO
using System.Xml;
using System.Text.Json;
using System.Text.Json.Serialization;

//Spark libraries
using Microsoft.Spark;
using Microsoft.Spark.Sql;




namespace Taxi_Spark_Algorithm
{

    
    
    class TaxiZoneData
    {
        public long zoneID { get; set; }
        public double zscore { get; set; }
    }

    class ComputationData
    {
        public ComputationData()
        {
            queryStartDateTime = "";
            queryEndDateTime = "";
            taxiZoneData = new List<TaxiZoneData>();
        }

        public string queryStartDateTime { get; set; }
        public string queryEndDateTime { get; set; }
        public List<TaxiZoneData> taxiZoneData { get; set; }

    }

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


            //list used to track output data
            ComputationData computationData = new ComputationData();

            List<NeighborData> neighbors = new List<NeighborData>();
            for (long i = 0; i < num_taxi_zones; ++i)
            {
                //generate neighbor list for current taxizone




                //query necessary data for neighbor list




                //Compute Getis-Ord Statistic and add z-score to output list
                computationData.taxiZoneData.Add(new TaxiZoneData() { zoneID = i, zscore = Getis_Ord.Getis_Ord_Stat(neighbors)});
                neighbors.Clear();

                //Console output to display computation 
                Console.Write(String.Format("\rProgress: {0}/{1} - {2:P}", i, num_taxi_zones, i/num_taxi_zones));
            }

            //output data data as JSON file
            string jsonString;
            jsonString = JsonSerializer.Serialize(computationData, new JsonSerializerOptions() { WriteIndented = true});
            File.WriteAllText("output.Json", jsonString);


        }


        //Initially created as serial algorithm 
        //TODO: Add parallelization
        //public static double Getis_Ord_Stat(List<NeighborData> neighbors)
        //{
        //    double _sum1 = 0, _sum2 = 0, _sum3 = 0, _X = 0, _S = 0;

        //    /*
        //    //Serial Version
        //    //compute sum 1
        //    double _sum1 = Sum1(ref neighbors);


        //    //compute sum 2
        //    double _sum2 = Sum2(ref neighbors);


        //    //compute sum 3
        //    double _sum3 = Sum3(ref neighbors);


        //    //compute X
        //    double _X = X(ref neighbors);


        //    //compute S
        //    double _S = S(ref neighbors, _X);
        //    */
        //    //Parallel Version

        //    //TaskFactory to launch tasks, list of tasks to for task management, uses default TaskScheduler
        //    List<Task<double>> tasks = new List<Task<double>>();
        //    TaskFactory<double> factory = new TaskFactory<double>();

        //    //start tasks for Sum1, SUm2, Sum3, X
        //    Task<double> T = factory.StartNew(()=>Sum1(ref neighbors));
        //    tasks.Add(T);

        //    T = factory.StartNew(() => Sum2(ref neighbors));
        //    tasks.Add(T);

        //    T = factory.StartNew(() => Sum3(ref neighbors));
        //    tasks.Add(T);

        //    T = factory.StartNew(() => X(ref neighbors));
        //    tasks.Add(T);

        //    //wait for all tasks to complete
        //    Task.WaitAll();

        //    //get results from tasks
        //    _sum1 = tasks[0].Result;
        //    _sum2 = tasks[1].Result;
        //    _sum3 = tasks[2].Result;
        //    _X = tasks[3].Result;

        //    //compute S
        //    _S = S(ref neighbors, _X);

        //    //compute stat and return
        //    double zscore = (_sum1 - (_X * _sum2)) / (_S * Math.Sqrt((neighbors.Count * _sum3 - _sum2) / (neighbors.Count-1)));
        //    return zscore;



        //}
        public static double IsNeighbor(double radius, double x1, double y1, double x2, double y2)
        {
            x1 = Math.Pow((x1 - x2),2);
            y1 = Math.Pow((y1 - y2),2);
            if (radius >= Math.Sqrt((x1 + y1))){
                return Math.Sqrt((x1 + y1));
            }
            return -1;
        }//NeighborData and TaxiZonedata could be used to provide coordinates. A radius should be some constant in main or global



        
        private static void BuildNeighborTable(int num_zones, double radius, ref SparkContext C, ref DataFrame Zone_Lookup)
        {//We require a table detailing a neighboring regions in the DB.
            //var path = @"file.csv";//<-Will need to change this for release.
            //string text = "100,200,1.4150000"; 
            //byte[] data = Encoding.ASCII.GetBytes(text); 
            double distance = 0.0;
            DataFrame df3 = Zone_Lookup;  //Intermediary dataframe
            DataFrame df4 = Zone_Lookup;//Intermediary dataframe
            
            //create two clones of Zone_Lookup
                                 
            using (StreamWriter output = new StreamWriter("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zoneNeighbors.csv")) {
                //Iteratively pull the coordinates from the taxi zone table. It only has num_zones items which should be 256 
                for (int i = 0; i <= num_zones; ++i)
                {
                    df3 = Zone_Lookup.Filter(String.Format("zoneID = {0}", i));
                    for (int j = 0; j <= num_zones; ++j)
                    {
                        df4 = Zone_Lookup.Filter(String.Format("zoneID = [0}", j));
                        distance = IsNeighbor(radius, (double)df3.First().Get(4), (double)df3.First().Get(5), (double)df4.First().Get(4), (double)df4.First().Get(5));
                        if (distance != -1 && i != j)
                        {
                            //text = i.ToString() + "," + j.ToString() + "," + distance.ToString() + "\n";
                            //data = Encoding.ASCII.GetBytes(text);
                            //File.WriteAllBytes(path, data);
                            //Write to file
                            output.WriteLine("{0},{1},{2}", i.ToString(), j.ToString(), distance);
                        }
                    }
                    //In a nested loop compare coordinates with all other regions which you also pull from the taxi zone table

                }
            }
            
        }//This function is intended to precompute neighbor relations which can be easily retrieved as distinct neighborhood lists given the current_id. These lists can be summed over to get the G* stat
        
        //The above function requires the neighbor relations to be written to a file instead of returning a dataframe. Dataframes should act like instantiated classes


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

    }







}

/* We need a method to grab the amount of incoming and outgoing traffic for a zone on a specific time. 
SELECT zoneID, cal_date, Count(*)
FROM(
SELECT zoneID,cal_id,trip_id 
FROM YELLOW


UNION

SELECT  zoneID,cal_id,trip_id 
FROM GREEN


UNION

SELECT  zoneID,cal_id,trip_id 
FROM HVF


UNION

SELECT  zoneID,cal_id,trip_id 
FROM HVFV

)
GROUP BY zoneID, cal_date 

//We need to split the time values apart with the calendar date and daily clock time separated if we want to do something like this.
*/
//Dataframe union and dataframe select are key











