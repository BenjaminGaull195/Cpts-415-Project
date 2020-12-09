using System;
using Microsoft.Spark.Sql;
using System.IO;
using Microsoft.Spark;


namespace BuildNeightborTest
{
    class Program
    {


        public static double IsNeighbor(double radius, double x1, double y1, double x2, double y2)
        {
            x1 = Math.Pow((x1 - x2), 2);
            y1 = Math.Pow((y1 - y2), 2);
            if (radius >= Math.Sqrt((x1 + y1)))
            {
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
            DataFrame df3 = Zone_Lookup;  //Intermediary dataframe
            DataFrame df4 = Zone_Lookup;//Intermediary dataframe

            //create two clones of Zone_Lookup

            using (StreamWriter output = new StreamWriter("/home/ubuntu/Cpts-415-Taxi-Spark-Data/taxi_zoneNeighbors.csv"))
            {
                output.WriteLine("currentZone,neighbor,distance");

                //Iteratively pull the coordinates from the taxi zone table. It only has num_zones items which should be 256 
                for (int i = 0; i <= num_zones; ++i)
                {
                    df3 = Zone_Lookup.Filter(String.Format("zoneID = {0}", i));
                    for (int j = 0; j <= num_zones; ++j)
                    {
                        df4 = Zone_Lookup.Filter(String.Format("zoneID = [0}", j));
                        distance = IsNeighbor(radius, Convert.ToDouble(df3.First().Get(4)), Convert.ToDouble(df3.First().Get(5)), Convert.ToDouble(df4.First().Get(4)), Convert.ToDouble(df4.First().Get(5)));
                        if (distance != -1 && i != j)
                        {
                            //text = i.ToString() + "," + j.ToString() + "," + distance.ToString() + "\n";
                            //data = Encoding.ASCII.GetBytes(text);
                            //File.WriteAllBytes(path, data);
                            //Write to file
                            output.WriteLine("{0},{1},{2}", i.ToString(), j.ToString(), distance);
                            Console.WriteLine("{0},{1}", i.ToString(), j.ToString());
                        }
                    }
                    //In a nested loop compare coordinates with all other regions which you also pull from the taxi zone table

                }
            }

        }
        //Console.WriteLine("Result/warning");	


        static void Main(string[] args)
        {
            Console.WriteLine("Startup");
            // Create a Spark session
            SparkSession spark = SparkSession
                .Builder()
                .AppName("word_count_sample")
                .GetOrCreate();
            Console.WriteLine("Context Created");

            // Create initial DataFrame
            DataFrame dataFrame = spark.Read().Option("header", true).Csv("/home/ubuntu/Cpts-415-Taxi-Spark-Data/TEST/taxi_zonesXY_test.csv");
            Console.WriteLine("Frames Created");
            BuildNeighborTable(10, 0.51, ref dataFrame);
            //


            // Stop Spark session
            spark.Stop();
        }
    }
}
