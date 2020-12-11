/*
  
  
 
 */



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

//JSON library to output data in a semi-structured format
//using Newtonsoft.Json;


namespace Taxi_Spark_Linux
{
    /// <summary>
    /// Used to store necessary neighbor data for easy access
    /// </summary>
    public class NeighborData
    {
        public int neighborID {get; set;}
        public int attribute { get; set; }
        public double distance { get; set; } 
    }

    /// <summary>
    /// used to store the output data for its respective taxi zone
    /// </summary>
    class TaxiZoneData
    {
        public long zoneID { get; set; }
        public double zscore { get; set; }
    }

    /// <summary>
    /// used to output entire data set to json file (Not Currently Working)
    /// </summary>
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
            SparkSession spark = SparkSession.Builder()
                .AppName("Taxi-Spark")
                .Config("spark.driver.memory", "8g")
                .Config("spark.executor.memory", "4g")
                .GetOrCreate();

            Stopwatch stopwatch = new Stopwatch();
            string elapsedTime = "";
            TimeSpan ts;

            //Build Dataset; TODO: Test reading multiple files from directory
            //Dataframes contain the data from the months of January - July.
            Console.WriteLine("Debug::");

            //Yellow Taxi Data Load and Debug Info
            stopwatch.Start();
            DataFrame YellowTaxi_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/yellow/");
            YellowTaxi_dataFrame.Cache();
            Console.WriteLine("Number of entries: {0}", YellowTaxi_dataFrame.Count());
            YellowTaxi_dataFrame.PrintSchema();
            YellowTaxi_dataFrame.Show();
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nyellow Taxi:");
            Console.WriteLine("Time to load Yellow Taxi Data: " + elapsedTime);

            //Green Taxi Data Load and Debug Info
            stopwatch.Start();
            DataFrame GreenTaxi_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/green/");
            GreenTaxi_dataFrame.Cache();
            Console.WriteLine("Number of entries: {0}", GreenTaxi_dataFrame.Count());
            GreenTaxi_dataFrame.PrintSchema();
            GreenTaxi_dataFrame.Show();
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nGreen Taxi:");
            Console.WriteLine("Time to load Green Taxi Data: " + elapsedTime);

            //FHV Data Load and Debug Info
            stopwatch.Start();
            DataFrame Fhv_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/fhv/");
            Fhv_dataFrame.Cache();
            Console.WriteLine("Number of entries: {0}", Fhv_dataFrame.Count());
            Fhv_dataFrame.PrintSchema();
            Fhv_dataFrame.Show();
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nFor Hire Vehicle:");
            Console.WriteLine("Time to load FHV Data: " + elapsedTime);

            //HVFHV Data Load and Debug Info
            stopwatch.Start();
            DataFrame Hvfhv_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/fhvhv/");
            Hvfhv_dataFrame.Cache();
            Console.WriteLine("Number of entries: {0}", Hvfhv_dataFrame.Count());
            Hvfhv_dataFrame.PrintSchema();
            Hvfhv_dataFrame.Show();
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nHigh Volume For Hire Vehicle:");
            Console.WriteLine("Time to load FHVHV Data: " + elapsedTime);

            //Taxi Zone Data Load and Debug Info
            stopwatch.Start();
            DataFrame Zonedata_dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zonesXY.csv");
            Zonedata_dataFrame.Cache();
            Console.WriteLine("Number of entries: {0}", Zonedata_dataFrame.Count());
            Zonedata_dataFrame.PrintSchema();
            Zonedata_dataFrame.Show();
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("\nTaxi Zones:");
            Console.WriteLine("Time to load Taxi Zone Data: " + elapsedTime);

            //If Neighbor Data does not exist, generate data
	        if(!File.Exists("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zoneNeighbors.csv")) 
            {
                Console.WriteLine("Could not find existing Neighbor Table data, building new file from Taxi Zone Data");
                stopwatch.Start();
	            //run BuildNeighborTable() function
                int numZones = (int)Zonedata_dataFrame.Count();
                BuildNeighborTable(numZones, 0.09, ref Zonedata_dataFrame);

                stopwatch.Stop();
                ts = stopwatch.Elapsed;
                elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
                Console.WriteLine("Time to build Taxi Zone Neighbor Data: " + elapsedTime);
            }

            //Neighbor Data Load and Debug Info
            stopwatch.Start();
            DataFrame NeighborTable = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zoneNeighbors.csv");
            NeighborTable.Cache();
            Console.WriteLine("\nDebug:: Neighbor Table:");
            Console.WriteLine("Number of entries: {0}", NeighborTable.Count());
            NeighborTable.PrintSchema();
            NeighborTable.Show();
            stopwatch.Stop();
            ts = stopwatch.Elapsed;
            elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Time to load Taxi Zone Neighbor Data: " + elapsedTime);
            
            //Initial queries to aquire necessary data
            int num_zones = (int)getNumTaxiZones(ref Zonedata_dataFrame);

            //ComputationData Class used to store data from computation for output
            ComputationData computationData = new ComputationData() { queryStartDateTime = "01/01/2020", queryEndDateTime = "06/30/2020"};
            
            //initialize list of NeighborData Classs to simplify data access during computation
            List<NeighborData> neighbors = new List<NeighborData>();

            Console.WriteLine("Beginning Computation");
            Console.WriteLine("LocationID,zscore");
            for (int i = 1; i <= num_zones; i++) {
                //get list of neighbors for current taxi zone
                neighbors = GetNeighbors(ref NeighborTable, ref YellowTaxi_dataFrame, ref GreenTaxi_dataFrame, ref Fhv_dataFrame, ref Hvfhv_dataFrame, i);

                //Console.WriteLine("ZoneID = {0} : ", i);
                //foreach (var cur in neighbors)
                //{
                //    Console.WriteLine("NeighborID={0}: Attribute={1}, Distance={2}", cur.neighborID, cur.attribute, cur.distance );
                //}
                //compute z-score and add results to ComputationData class for output
                //write to console
                Console.Write("{0},", i);

                double temp = Getis_Ord_Stat(neighbors);
                computationData.taxiZoneData.Add(new TaxiZoneData() { zoneID = i, zscore = temp});
                
                //clear neighbor list to reduce risk of memory leaks
                neighbors.Clear();

                //Console.Write(String.Format("\rProgress: {0}/{1} - {2:P}", i, num_taxi_zones, i/num_taxi_zones));
            }
            Console.WriteLine("Computation Complete;\nWriting output to: output.json");

            //output data to json file
            //string jsonString;
            //jsonString = JsonConvert.SerializeObject(computationData, Formatting.Indented);
            //File.WriteAllText("output.Json", jsonString);

        }

        /*
        //Compute Getis-Ord Statistic and add z-score to output list
                computationData.taxiZoneData.Add(new TaxiZoneData() { zoneID = i, zscore = Getis_Ord.Getis_Ord_Stat(neighbors)});
                neighbors.Clear();

                //Console output to display computation 
                Console.Write(String.Format("\rProgress: {0}/{1} - {2:P}", i, num_taxi_zones, i/num_taxi_zones));

        */
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
        public static List<NeighborData> GetNeighbors(ref DataFrame NeighborTable, ref DataFrame YellowTaxi_dataFrame, ref DataFrame GreenTaxi_dataFrame, ref DataFrame Fhv_dataFrame, ref DataFrame Hvfhv_dataFrame, int currentZoneID)
        {
            DataFrame temp = NeighborTable.Where(String.Format("currentZone = '{0}'", currentZoneID));
            
            //get collection of taxi zones considered to be neighbors to current zone
            Row [] neighbors = temp.Collect().ToArray<Row>();
            long attr_count = 0;
            List<NeighborData> neighborData = new List<NeighborData>();
            //Console.WriteLine("Num Neighbors = {0}", neighbors.Length); 
            foreach(Row cur in neighbors) 
            {
                //Convert each Apache.Spark.Sql.Row to equivalent Neighbordata
                //Console.WriteLine("NeighborID={0}", cur.Get(1));
                attr_count = (getNumYellowTrips(ref YellowTaxi_dataFrame, Convert.ToInt32(cur.Get(1))) + getNumGreenTrips(ref GreenTaxi_dataFrame, Convert.ToInt32(cur.Get(1))) + getNumFHVTrips(ref Fhv_dataFrame, Convert.ToInt32(cur.Get(1))) + getNumHVFHVTrips(ref Hvfhv_dataFrame, Convert.ToInt32(cur.Get(1))));
                neighborData.Add(new NeighborData(){neighborID = Convert.ToInt32(cur.Get(1)), distance = Convert.ToDouble(cur.Get(2)), attribute = (int)attr_count});
            }
            //return list of neighbors
            return neighborData;
        }

        /// <summary>
        /// Returns the total number of yellow taxi trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="Yellow_Tripdata">pass by ref variable refering to yellow_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns>Returns the total number of Yellow Taxi trips to/from the current ZoneID</returns>
        public static long getNumYellowTrips(ref DataFrame Yellow_Tripdata, int currentZoneID)
        {
            //Console.WriteLine("Counting number of Yellow Trips");
            long temp = Yellow_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
            //Console.WriteLine("Number of Yellow Trips = {0}", temp);
            return temp;
        }

        /// <summary>
        /// Returns the total number of green taxi trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="Green_Tripdata">pass by ref variable refering to green_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns>Returns the total number of Green Taxi trips to/from the current ZoneID</returns>
        public static long getNumGreenTrips(ref DataFrame Green_Tripdata, int currentZoneID)
        {
            //Console.WriteLine("Counting number of Green Trips");
            long temp = Green_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
            //Console.WriteLine("Number of Green Trips = {0}", temp);
            return temp;
        }

        /// <summary>
        /// Returns the total number of fhv trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="FHV_Tripdata">pass by ref variable refering to fhv_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns>Returns the total number of FHV trips to/from the current ZoneID</returns>
        public static long getNumFHVTrips(ref DataFrame FHV_Tripdata, int currentZoneID)
        {
            //Console.WriteLine("Counting number of FHV Trips");
            long temp = FHV_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
            //Console.WriteLine("Number of Yellow Trips = {0}", temp);
            return temp;
        }

        /// <summary>
        /// Returns the total number of hvfhv trips that have a pickup/dropoff that match the currentZoneID
        /// </summary>
        /// <param name="HVFHV_Tripdata">pass by ref variable refering to hvfhv_tripdata dataframe</param>
        /// <param name="currentZoneID">ID for the current taxi zone</param>
        /// <returns>Returns the total number of HVFHV trips to/from the current ZoneID</returns>
        public static long getNumHVFHVTrips(ref DataFrame HVFHV_Tripdata, int currentZoneID)
        {
            //Console.WriteLine("Counting number of HVFHV Trips");
            long temp = HVFHV_Tripdata.Filter(String.Format("PULocationID = '{0}' OR DOLocationID = '{0}'", currentZoneID)).Count();
            //Console.WriteLine("Number of Yellow Trips = {0}", temp);
            return temp;
        }


        /// <summary>
        /// Calculates a zscore statistic for the current taxi zone based on data from its neighbors
        /// </summary>
        /// <param name="neighbors">List of NeighborData for current Taxi Zone</param>
        /// <returns>Returns the zscore for the current zone</returns>
        public static double Getis_Ord_Stat(List<NeighborData> neighbors)
        {
            double _sum1 = 0, _sum2 = 0, _sum3 = 0, _X = 0, _S = 0;

            /*
            //Serial Version
            //compute sum 1
            double _sum1 = Sum1(ref neighbors);


            //compute sum 2
            double _sum2 = Sum2(ref neighbors);


            //compute sum 3
            double _sum3 = Sum3(ref neighbors);


            //compute X
            double _X = X(ref neighbors);


            //compute S
            double _S = S(ref neighbors, _X);
            */
            //Parallel Version

            //TaskFactory to launch tasks, list of tasks to for task management, uses default TaskScheduler
            List<Task<double>> tasks = new List<Task<double>>();
            TaskFactory<double> factory = new TaskFactory<double>();

            //start tasks for Sum1, SUm2, Sum3, X
            Task<double> T = factory.StartNew(() => Sum1(ref neighbors));
            tasks.Add(T);

            T = factory.StartNew(() => Sum2(ref neighbors));
            tasks.Add(T);

            T = factory.StartNew(() => Sum3(ref neighbors));
            tasks.Add(T);

            T = factory.StartNew(() => X(ref neighbors));
            tasks.Add(T);

            //wait for all tasks to complete
            Task.WaitAll();

            //get results from tasks
            _sum1 = tasks[0].Result;
            _sum2 = tasks[1].Result;
            _sum3 = tasks[2].Result;
            _X = tasks[3].Result;

            double temp1 = _X * _sum2;
            double temp2 = (_sum1 - temp1);

            Console.Write("{0} / (", temp2);
            //compute S
            _S = S(ref neighbors, _X);

            //compute stat and return
            
            double temp3 = neighbors.Count * _sum3;
            double temp4 = Math.Pow(_sum2, 2);
            double temp5 = (temp3 - temp4);
            double temp6 = (neighbors.Count - 1);
            double temp7 = (_S * Math.Sqrt( temp5 / temp6));
            double zscore = temp2 / temp7;
            Console.WriteLine(") * sqrt({0} / {1}))", temp5, temp6);
            return zscore;
        }


        /// <summary>
        /// Calculates the sum of the product of the neighbors distance and attribute weight
        /// </summary>
        /// <param name="neighbors">List of NeighborData</param>
        /// <returns></returns>
        public static double Sum1(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.distance * data.attribute;
            }

            return sum;
        }

        /// <summary>
        /// Calculates the sum of the niehgbors distance
        /// </summary>
        /// <param name="neighbors">List of NeighborData</param>
        /// <returns></returns>
        public static double Sum2(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.distance;
            }

            return sum;
        }

        /// <summary>
        /// Calculates the sum of the square of the neighbors distance
        /// </summary>
        /// <param name="neighbors">List of NeighborData</param>
        /// <returns></returns>
        public static double Sum3(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += (data.distance * data.distance);
            }

            return sum;
        }

        /// <summary>
        /// Calculates the average distance of the neighbors
        /// </summary>
        /// <param name="neighbors">List of NeighborData</param>
        /// <returns></returns>
        public static double X(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.attribute;
            }

            return sum / neighbors.Count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="neighbors">List of NeighborData</param>
        /// <param name="X">Average distance of neighbors</param>
        /// <returns></returns>
        public static double S(ref List<NeighborData> neighbors, double X)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += (data.attribute * data.attribute);
            }
            double temp1 = (sum / neighbors.Count);
            double temp2 = Math.Pow(X, 2);
            double temp3 =  Math.Sqrt(temp1 - temp2);
            Console.Write("(sqrt({0} - {1})", temp1, temp2);
            return temp3;
        }


        /// <summary>
        /// Determines of a taxizone wi within a certain distance of another
        /// </summary>
        /// <param name="radius">maximum distance</param>
        /// <param name="x1">x coordinate from taxi zone 1</param>
        /// <param name="y1">y coordinate from taxi zone 1</param>
        /// <param name="x2">x coordinate from taxi zone 2</param>
        /// <param name="y2">y coordinate from taxi zone 2</param>
        /// <returns>returns distance if within radius, else returns -1</returns>
        public static double IsNeighbor(double radius, double x1, double y1, double x2, double y2)
        {
            x1 = Math.Pow((x1 - x2),2);
            y1 = Math.Pow((y1 - y2),2);
            if (radius >= Math.Sqrt((x1 + y1))){
                return Math.Sqrt((x1 + y1));
            }
            return -1;
        }	

        /// <summary>
        /// Builds a csv file containing the neighbor relational data
        /// </summary>
        /// <param name="num_zones">number of zones</param>
        /// <param name="radius">maximum distance for neighbors</param>
        /// <param name="Zone_Lookup">reference to Taxi_zone dataframe</param>
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
                int numNeighbors = 0;
                Console.WriteLine("Right before iteration");
                //Iteratively pull the coordinates from the taxi zone table. It only has num_zones items which should be 256 
                for (int i = 0; i < num_zones; ++i)
                {
                    numNeighbors = 0;
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
                            numNeighbors++;
                            output.WriteLine("{0},{1},{2}", (i+1).ToString(), (j+1).ToString(), distance);
                            //Console.WriteLine("{0},{1},{2}",i.ToString(), j.ToString(),distance.ToString());
                        }
                    }
                    Console.WriteLine("Debug:: num neighbors for zone {0} = {1}", i, numNeighbors);
                    //In a nested loop compare coordinates with all other regions which you also pull from the taxi zone table
                }
            }
	
        }
    }
}
