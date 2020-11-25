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


    class Program
    {
        static void Main(string[] args)
        {
            //Initialize Spark SQL session 



            //Build Dataset
                       
            

            //Initial queries
            int num_taxi_zones = 0;
                       

            //list used to track output data
            List<TaxiZoneData> zoneScore = new List<TaxiZoneData>();

            for (int i = 0; i < num_taxi_zones; ++i)
            {
                //Query Data





                //Compute Getis-Ord Statistic and add z-score to output list
                zoneScore.Add(new TaxiZoneData() { zoneID = i, zscore = Getis_Ord_Stat()});
            }

            //output data



        }


        //Initially created as serial algorithm 
        //TODO: Add parallelization
        public static double Getis_Ord_Stat(List<NeighborData> neighbors)
        {
            //compute sum 1
            int _sum1 = Sum1(ref neighbors);


            //compute sum 2
            int _sum2 = Sum2(ref neighbors);


            //compute sum 3
            int _sum3 = Sum3(ref neighbors);


            //compute X
            double _X = X(ref neighbors);


            //compute S
            double _S = S(ref neighbors, _X);


            //compute stat and return



        }


        private static int Sum1(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum;
        }

        private static int Sum2(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum;
        }

        private static int Sum3(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum;
        }

        private static double X(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum / neighbors.Count;
        }

        private static double S(ref List<NeighborData> neighbors, double X)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }


            return sum;
        }




    }


    

}
