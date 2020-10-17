using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;

namespace hvfhv_Parser
{
    class Program
    {
        static void Main(string[] args)
        {
            // Read path to directory containing all hvfhv csv files
            Console.Write("Enter Path for data files: ");
            string path_str = Console.ReadLine();

            //get list of filenames in dir
            DirectoryInfo hvfhv_dir = new DirectoryInfo(path_str);
            FileInfo[] files = hvfhv_dir.GetFiles();

            //stopwatch to track execution time of parser
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            long tuple_count = 0;
            int file_index = 0;
            //output filestream
           
                               
            // executes the following code for each file
            foreach (FileInfo cur in files)
            {
                using (StreamWriter file = new StreamWriter(path_str + string.Format("\\hvfhv_insert_{0}.sql", file_index)))
                {
                    //reads all lines from the file
                    string[] data = File.ReadAllLines(cur.FullName);
                    //get column count, used to check for out-of-bounds errors
                    string[] data_header = data[1].Split(',');

                    // executes the following code for each string in data
                    foreach (string entry in data.Skip(1))
                    {
                        //calles the parse function, then adds the parsed string the the output list
                        file.WriteLine(parse(entry, data_header.Length));
                        ++tuple_count;
                    }
                }
                ++file_index;
            }

                
            stopwatch.Stop();
            
            TimeSpan timeSpan = stopwatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds,
                timeSpan.Milliseconds / 10);
            Console.WriteLine("RunTime " + elapsedTime);
            Console.WriteLine("Tuples : {0}", tuple_count);

        }


        //used to parse the data
        private static string parse(string csv, int numValues)
        {
            string sql_str = "";

            //parses the string by the delimiter
            string[] parsed_data = csv.Split(',');

            //inserts parsed data into formatted sql string
            /* Alternate Format Strings:
             * FHV:         "INSERT INTO FHV_Trip_Records(Dispatch_base_num, Pickup_datetime, Dropoff_datetime, PULocationID, DOLocationID, SR_Flag) VALUES (\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\');", parsed_data[0], parsed_data[1], parsed_data[2], parsed_data[3], parsed_data[4], parsed_data[5]
             * HV-FHV:      "INSERT INTO HV_FHV_Trip_Records(Hvfhs_license_num, Dispatch_base_num, Pickup_datetime, Dropoff_datetime, PULocationID, DOLocationID, SR_Flag) VALUES (\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\');", parsed_data[0], parsed_data[1], parsed_data[2], parsed_data[3], parsed_data[4], parsed_data[5], parsed_data[6]
             * Yellow:      "INSERT INTO HV_FHV_Trip_Records(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, Passenger_Count,Trip_Distance, RateCodeID, Store_and_fwd_flag, PULocationID, DOLocationID, Payment_type, Fare_ammount, Extra, MTA_tax, Tip_Amount, Tolls_amount, Improvement_surcharge, Total_amount, congestion_surcharge) VALUES (\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\', \'{8}\', \'{9}\', \'{10}\', \'{11}\', \'{12}\', \'{13}\', \'{14}\', \'{15}\', \'{16}\', \'{17}\');", parsed_data[0], parsed_data[1], parsed_data[2], parsed_data[3], parsed_data[4], parsed_data[5], parsed_data[6], parsed_data[7], parsed_data[8], parsed_data[9],parsed_data[10], parsed_data[11], parsed_data[12], parsed_data[13], parsed_data[14], parsed_data[15], parsed_data[16], parsed_data[17]
             * Green:       "INSERT INTO HV_FHV_Trip_Records(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, Passenger_Count,Trip_Distance, RateCodeID, Store_and_fwd_flag, PULocationID, DOLocationID, Fare_ammount, Extra, MTA_tax, Tip_Amount, Tolls_amount, Improvement_surcharge, Total_amount, Payment_type, Trip_type, congestion_surcharge) VALUES (\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\', \'{8}\', \'{9}\', \'{10}\', \'{11}\', \'{12}\', \'{13}\', \'{14}\', \'{15}\', \'{16}\', \'{17}\', \'{18}\');", parsed_data[0], parsed_data[1], parsed_data[2], parsed_data[3], parsed_data[4], parsed_data[5], parsed_data[6], parsed_data[7], parsed_data[8], parsed_data[9],parsed_data[10], parsed_data[11], parsed_data[12], parsed_data[13], parsed_data[14], parsed_data[15], parsed_data[16], parsed_data[17], parsed_data[18]
             * Taxi Zones:  "INSERT INTO HV_FHV_Trip_Records() VALUES ();",
            */
            sql_str = string.Format("INSERT INTO HV_FHV_Trip_Records(Hvfhs_license_num, Dispatch_base_num, Pickup_datetime, Dropoff_datetime, PULocationID, DOLocationID, SR_Flag) VALUES (\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\');", parsed_data[0], parsed_data[1], parsed_data[2], parsed_data[3], parsed_data[4], parsed_data[5], parsed_data[6]);
            
            //return formatted string
            return sql_str;
        }

    }
}
