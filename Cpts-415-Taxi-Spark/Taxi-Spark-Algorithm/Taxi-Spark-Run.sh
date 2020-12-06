#stops and starts the spark DBMS
~/bin/spark-3.0.1-bin-hadoop2.7/sbin/stop-all.sh
~/bin/spark-3.0.1-bin-hadoop2.7/sbin/start-all.sh




#build application - ensures we run the most recent
dotnet build

cd bin/Debug/net5.0/

#Wait for user input to progres to execution
echo "\nPress Enter to continue"
read -s


~/bin/spark-3.0.1-bin-hadoop2.7/bin/spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master local \
microsoft-spark-3-0_2.12-1.0.0.jar \
dotnet Taxi-Spark-Algorithm.dll 