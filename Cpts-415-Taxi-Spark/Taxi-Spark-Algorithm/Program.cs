﻿using System;
using System.Threading;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;

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



            //compute sum 2



            //compute sum 3



            //compute X



            //compute S



            //compute stat and return



        }


        private int Sum1(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum;
        }

        private int Sum2(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum;
        }

        private int Sum3(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum;
        }

        private double X(ref List<NeighborData> neighbors)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }

            return sum / neighbors.Count;
        }

        private double S(ref List<NeighborData> neighbors, double X)
        {
            int sum = 0;

            foreach (NeighborData data in neighbors)
            {

            }


            return sum;
        }




    }


    

}
