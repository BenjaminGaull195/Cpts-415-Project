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
            //output filestream
            using (StreamWriter file = new StreamWriter(path_str + "\\hvfhv_insert.sql"))
            {

                
                
                // executes the following code for each file
                foreach (FileInfo cur in files)
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
                    }

                }

                //write list of all formatted strings to sql file
                //File.WriteAllLines(path_str + "\\hvfhv_insert.sql", parsed_data);


                stopwatch.Stop();
            }
            TimeSpan timeSpan = stopwatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds,
                timeSpan.Milliseconds / 10);
            Console.WriteLine("RunTime " + elapsedTime);


        }


        //used to parse the data
        private static string parse(string csv, int numValues)
        {
            string sql_str = "";

            //parses the string by the delimiter
            string[] parsed_data = csv.Split(',');

            //inserts parsed data into formatted sql string
            sql_str = string.Format("INSERT INTO HV_FHV_Trip_Records(Hvfhs_license_num, Dispatch_base_num, Pickup_datetime, Dropoff_datetime, PULocationID, DOLocationID, SR_Flag) VALUES (\'{0}\', \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\');", parsed_data[0], parsed_data[1], parsed_data[2], parsed_data[3], parsed_data[4], parsed_data[5], parsed_data[6]);
            
            //return formatted string
            return sql_str;
        }

    }
}
