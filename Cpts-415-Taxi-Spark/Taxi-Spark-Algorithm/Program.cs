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


//Formatted/Semi-structured IO
using System.Xml;
using System.Text.Json;
using System.Text.Json.Serialization;

//Spark libraries
using Microsoft.Spark;
using Microsoft.Spark.Sql;




namespace Taxi_Spark_Algorithm
{

    class NeighborData : IDisposable
    {
        public int attribute { get; set; }
        public double distance { get; set; }
        public void Dispose()
        {

        }
    }
    
    class TaxiZoneData
    {
        public int zoneID { get; set; }
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
            SparkSession spark = new SparkSession();
            Dataframe dataFrame = spark.read().Csv(input);


            //Build Dataset
                       
            

            //Initial queries
            int num_taxi_zones = 0;


            //list used to track output data
            ComputationData computationData = new ComputationData();

            List<NeighborData> neighbors = new List<NeighborData>();
            for (int i = 0; i < num_taxi_zones; ++i)
            {
               //Query for current taxi zone




                //Compute Getis-Ord Statistic and add z-score to output list
                computationData.taxiZoneData.Add(new TaxiZoneData() { zoneID = i, zscore = Getis_Ord_Stat(neighbors)});
                neighbors.Clear();
            }

            //output data
            string jsonString;
            jsonString = JsonSerializer.Serialize(computationData, new JsonSerializerOptions() { WriteIndented = true});
            File.WriteAllText("output.Json", jsonString);


        }


        //Initially created as serial algorithm 
        //TODO: Add parallelization
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
            Task<double> T = factory.StartNew(()=>Sum1(ref neighbors));
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

            //compute S
            _S = S(ref neighbors, _X);

            //compute stat and return
            double zscore = (_sum1 - (_X * _sum2)) / (_S * Math.Sqrt((neighbors.Count * _sum3 - _sum2) / (neighbors.Count-1)));
            return zscore;



        }
        public static bool IsNeighbor(double radius, double x1, double y1, double x2, double y2)
        {
            x1 = Math.Pow((x1 - x2),2);
            y1 = Math.Pow((y1 - y2),2);
            if (radius >= Math.Sqrt((x1 + y1))){
                return true;
            }
            return false;
        }//NeighborData and TaxiZonedata could be used to provide coordinates. A radius should be some constant in main or global

        private static double Sum1(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.distance * data.attribute;
            }

            return sum;
        }

        private static double Sum2(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.distance;
            }

            return sum;
        }

        private static double Sum3(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += (data.distance * data.distance);
            }

            return sum;
        }

        private static double X(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.attribute;
            }

            return sum / neighbors.Count;
        }

        private static double S(ref List<NeighborData> neighbors, double X)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += (data.attribute * data.attribute);
            }

            return Math.Sqrt((sum/neighbors.Count) - Math.Pow(X, 2));
        }

        /*
        private static void BuildNeighborTable(int num_zones, double radius, SparkContext C){//We require a table detailing a neighboring regions in the DB.
            DataFrame df; //The dataframe holding the taxizone lookup table 
            DataFrame df2; //The table being accumulated with neighbor relations.
            DataFrame df3; //Intermediary dataframe
            DataFrame df4; //Intermediary dataframe
            //Iteratively pull the coordinates from the taxi zone table. It only has num_zones items which should be 256 
            for(int i = 0; i <= num_zones; ++i)
                {
                    df3 = df.Filter(df("ZoneID") == i).Show(0);
                    for(int j = 0; j <= num_zones; ++j)
                    {
                        df4 = df.Filter(df("ZoneID") == i).Show(0);

                        if(IsNeighbor(radius,,,,) && i != j){//Need help extracting values from the coordinates from df3 and df4.
                            df4 = C.(
                            (i, j)
                            ).toDF("Current_ID", "Neighbor_ID");//The zone ids within df3 and df4
                            df2.Union(df4).Show(0);
                        }
                        //In a nested loop compare coordinates with all other regions which you also pull from the taxi zone table
                    }
                }
        }//This function is intended to precompute neighbor relations which can be easily retrieved as distinct neighborhood lists given the current_id. These lists can be summed over to get the G* stat*/


    }


    

}

/* We need a method to grab the amount of incoming and outgoing traffic for a zone on a specific time. 
SELECT zoneID, cal_date, Count(*)
FROM(

SELECT * 
FROM YELLOW
WHERE ((pickup_zone = x OR dropoff_zone = x) AND (pickup_date = y OR dropoff_date = y))

UNION

SELECT * 
FROM GREEN
WHERE ((pickup_zone = x OR dropoff_zone = x) AND (pickup_date = y OR dropoff_date = y))

UNION

SELECT * 
FROM HVF
WHERE ((pickup_zone = x OR dropoff_zone = x) AND (pickup_date = y OR dropoff_date = y))

UNION

SELECT * 
FROM HVFV
WHERE ((pickup_zone = x OR dropoff_zone = x) AND (pickup_date = y OR dropoff_date = y))

)
GROUP BY zoneID, cal_date 

//We need to split the time values apart with the calendar date and daily clock time separated.
*/