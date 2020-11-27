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

            try
            {
                double sum1 = Getis_Ord.Sum1(ref testData);
                Assert.AreEqual(sum1, );
            }
            catch (AssertFailedException afe)
            {
                Console.WriteLine(afe.Message.ToString());
            }
        }

        [TestMethod]
        public void TestSum2()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            try
            { 
                double sum2 = Getis_Ord.Sum1(ref testData);
                Assert.AreEqual(sum2, );
            }
            catch (AssertFailedException afe)
            {
                Console.WriteLine(afe.Message.ToString());
            }
        }

        [TestMethod]
        public void TestSum3()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            try
            { 
                double sum3 = Getis_Ord.Sum1(ref testData);
                Assert.AreEqual(sum3, );
            }
            catch (AssertFailedException afe)
            {
                Console.WriteLine(afe.Message.ToString());
            }
        }

        [TestMethod]
        public void TestX()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            try
            { 
                double X = Getis_Ord.X(ref testData);
                Assert.AreEqual(X, );
            }
            catch (AssertFailedException afe)
            {
                Console.WriteLine(afe.Message.ToString());
            }
        }

        [TestMethod]
        public void TestS()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            try
            { 
                double X = Getis_Ord.X(ref testData);
                double S = Getis_Ord.S(ref testData, X);
                Assert.AreEqual(S, );
            }
            catch (AssertFailedException afe)
            {
                Console.WriteLine(afe.Message.ToString());
            }
        }

        [TestMethod]
        public void TestGetis_Ord()
        {
            List<NeighborData> testData = new List<NeighborData>();
            testData.Add(new NeighborData() { attribute = 17, distance = 1.8 });
            testData.Add(new NeighborData() { attribute = 12, distance = 2.1 });
            testData.Add(new NeighborData() { attribute = 31, distance = 0.7 });
            testData.Add(new NeighborData() { attribute = 15, distance = 1.5 });

            try { 
                double zscore = Getis_Ord.Getis_Ord_Stat(testData);
                Assert.AreEqual(zscore, );
            }
            catch (AssertFailedException afe)
            {
                Console.WriteLine(afe.Message.ToString());
            }

        }

    }
}
