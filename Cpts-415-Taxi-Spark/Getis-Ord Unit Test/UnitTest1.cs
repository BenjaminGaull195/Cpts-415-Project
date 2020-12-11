using Microsoft.VisualStudio.TestTools.UnitTesting;
using Algorithms;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;


namespace Getis_Ord_Unit_Test
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestSum1()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8});
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1});
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7});
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5});

            Console.WriteLine("Sum1() Test:");
            Console.WriteLine("\tTest Data:");
            for(int i = 0; i < testData.Count; ++i)
            {
                Console.WriteLine("\t\tNeighbor {0}: Attribute={1}, Distance={2}", i, testData[i].attribute, testData[i].distance);
            }

            
            
            double sum1 = Getis_Ord.Sum1(ref testData);
            Console.WriteLine("\tSum1 = {0}", sum1);
            Assert.AreEqual(100.0, sum1);
            
            
        }

        [TestMethod]
        public void TestSum2()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            Console.WriteLine("Sum2() Test:");
            Console.WriteLine("\tTest Data:");
            for (int i = 0; i < testData.Count; ++i)
            {
                Console.WriteLine("\t\tNeighbor {0}: Attribute={1}, Distance={2}", i, testData[i].attribute, testData[i].distance);
            }

            
            
            double sum2 = Getis_Ord.Sum2(ref testData);
            Console.WriteLine("\tSum2 = {0}", sum2);
            Assert.AreEqual(6.1, sum2);
            
            
        }

        [TestMethod]
        public void TestSum3()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            Console.WriteLine("Sum3() Test:");
            Console.WriteLine("\tTest Data:");
            for (int i = 0; i < testData.Count; ++i)
            {
                Console.WriteLine("\t\tNeighbor {0}: Attribute={1}, Distance={2}", i, testData[i].attribute, testData[i].distance);
            }

            
            
            double sum3 = Getis_Ord.Sum3(ref testData);
            Console.WriteLine("\tSum3 = {0}", sum3);
            Assert.AreEqual(10.39, sum3);
            
            
        }

        [TestMethod]
        public void TestX()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            Console.WriteLine("X() Test:");
            Console.WriteLine("\tTest Data:");
            for (int i = 0; i < testData.Count; ++i)
            {
                Console.WriteLine("\t\tNeighbor {0}: Attribute={1}, Distance={2}", i, testData[i].attribute, testData[i].distance);
            }

            
            
            double X = Getis_Ord.X(ref testData);
            Console.WriteLine("\tX = {0}", X);
            Assert.AreEqual(18.75, X);
            
            
        }

        [TestMethod]
        public void TestS()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            Console.WriteLine("S() Test:");
            Console.WriteLine("\tTest Data:");
            for (int i = 0; i < testData.Count; ++i)
            {
                Console.WriteLine("\t\tNeighbor {0}: Attribute={1}, Distance={2}", i, testData[i].attribute, testData[i].distance);
            }

            
            
            double X = Getis_Ord.X(ref testData);
            double S = Getis_Ord.S(ref testData, X);
            Console.WriteLine("\tS = {0}", S);
            Assert.AreEqual(7.292976073, S);
            
            
        }

        [TestMethod]
        public void TestGetis_Ord()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            Console.WriteLine("Getis_Ord_Stat() Test:");
            Console.WriteLine("\tTest Data:");
            for (int i = 0; i < testData.Count; ++i)
            {
                Console.WriteLine("\t\tNeighbor {0}: Attribute={1}, Distance={2}", i, testData[i].attribute, testData[i].distance);
            }

            double zscore = Getis_Ord.Getis_Ord_Stat(testData);
            Console.WriteLine("\tzscore = {0}", zscore);
            Assert.AreEqual(-1.636888371, zscore);
            
            

        }

    }
}
