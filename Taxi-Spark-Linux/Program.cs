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

            string datapath = "/home/ubuntu/Cpts-415-Taxi-Spark-Data/TEST/";
            //Build Dataset
            DataFrame YellowTaxi_dataFrame = spark.Read().Option("header", true).Csv(datapath + "yellow_tripdata_2020-01test.csv");
            DataFrame GreenTaxi_dataFrame = spark.Read().Option("header", true).Csv(datapath + "green_tripdata_2020-01test.csv");
            DataFrame Fhv_dataFrame = spark.Read().Option("header", true).Csv(datapath + "fhv_tripdata_2020-01test.csv");
            DataFrame Hvfhv_dataFrame = spark.Read().Option("header", true).Csv(datapath + "fhvhv_tripdata_2020-01test.csv");
            DataFrame Zonedata_dataFrame = spark.Read().Option("header", true).Csv(datapath + "zones_data.csv");

            //Unioned the above dataframes with temporary ones containing the rest of the data

            //DataFrame temp = spark.Read().Csv("yellow_tripdata_2020-02test.csv");
            //YellowTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("yellow_tripdata_2020-03test.csv");
            //YellowTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("yellow_tripdata_2020-04test.csv");
            //YellowTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("yellow_tripdata_2020-05test.csv");
            //YellowTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("yellow_tripdata_2020-06test.csv");
            //YellowTaxi_dataFrame.Union(temp);

            //temp = spark.Read().Csv("green_tripdata_2020-02test.csv");
            //GreenTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("green_tripdata_2020-03test.csv");
            //GreenTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("green_tripdata_2020-04test.csv");
            //GreenTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("green_tripdata_2020-05test.csv");
            //GreenTaxi_dataFrame.Union(temp);
            //temp = spark.Read().Csv("green_tripdata_2020-06test.csv");
            //GreenTaxi_dataFrame.Union(temp);

            //temp = spark.Read().Csv("fhv_tripdata_2020-02test.csv");
            //Fhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhv_tripdata_2020-03test.csv");
            //Fhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhv_tripdata_2020-04test.csv");
            //Fhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhv_tripdata_2020-05test.csv");
            //Fhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhv_tripdata_2020-06test.csv");
            //Fhv_dataFrame.Union(temp);

            //temp = spark.Read().Csv("fhvhv_tripdata_2020-02test.csv");
            //Hvfhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhvhv_tripdata_2020-03test.csv");
            //Hvfhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhvhv_tripdata_2020-04test.csv");
            //Hvfhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhvhv_tripdata_2020-05test.csv");
            //Hvfhv_dataFrame.Union(temp);
            //temp = spark.Read().Csv("fhvhv_tripdata_2020-06test.csv");
            //Hvfhv_dataFrame.Union(temp);


            //Dataframes contain the data from the months of January - July.





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

        }
    }
}
