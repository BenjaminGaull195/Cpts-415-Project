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

    class NeighborData
    {
        public int attribute { get; set; }
        public double distance { get; set; }
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

        }


        List<TaxiZoneData> taxiZoneData;

    }

    class Program
    {
        static void Main(string[] args)
        {
            //Initialize Spark SQL session 



            //Build Dataset
                       
            

            //Initial queries
            int num_taxi_zones = 0;
                       

            //list used to track output data
            ComputationData computationData = 

            for (int i = 0; i < num_taxi_zones; ++i)
            {
                //Query Data





                //Compute Getis-Ord Statistic and add z-score to output list
                zoneScore.Add(new TaxiZoneData() { zoneID = i, zscore = Getis_Ord_Stat()});
            }

            //output data
            string jsonString;
            jsonString = JsonSerializer.Serialize(zoneScore);
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
            List<Task<double>> tasks = new List<Task<double>>();

            TaskFactory<double> factory = new TaskFactory<double>();


            //compute stat and return
            
            double zscore = (_sum1 - (_X * _sum2)) / (_S * Math.Sqrt((neighbors.Count * _sum3 - _sum2) / (neighbors.Count-1)));
            return zscore;



        }


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




    }


    

}
