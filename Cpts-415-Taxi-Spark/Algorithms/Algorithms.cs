using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Algorithms
{

    public class NeighborData
    {
        public int attribute { get; set; }
        public double distance { get; set; }

    }



    public static class Getis_Ord
    {

        //Implemented Parallel Algorithm Implementation 
        //TODO: Test Algorithm
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

            //compute S
            _S = S(ref neighbors, _X);

            //compute stat and return
            double zscore = (_sum1 - (_X * _sum2)) / (_S * Math.Sqrt((neighbors.Count * _sum3 - _sum2) / (neighbors.Count - 1)));
            return zscore;



        }



        public static double Sum1(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.distance * data.attribute;
            }

            return sum;
        }

        public static double Sum2(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.distance;
            }

            return sum;
        }

        public static double Sum3(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += (data.distance * data.distance);
            }

            return sum;
        }

        public static double X(ref List<NeighborData> neighbors)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += data.attribute;
            }

            return sum / neighbors.Count;
        }

        public static double S(ref List<NeighborData> neighbors, double X)
        {
            double sum = 0;

            foreach (NeighborData data in neighbors)
            {
                sum += (data.attribute * data.attribute);
            }

            return Math.Sqrt((sum / neighbors.Count) - Math.Pow(X, 2));
        }



    }
}
