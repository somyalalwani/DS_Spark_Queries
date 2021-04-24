from pyspark.sql import SparkSession
import sys

arg_count = len(sys.argv)
try:
	if (int(sys.argv[1])<1):
		print("Invalid number of CPUs")
		exit()
	if arg_count!=3:
		print("Invalid number of arguments!!")
		exit()

except:
	print("Invalid number of CPUs")
	exit()


cpu = "local[" + str(sys.argv[1]) + "]"
spark = SparkSession.builder.master(cpu).getOrCreate()
df= spark.read.csv("airports.csv",header=True)
df.createGlobalTempView("airport_data")


ans=spark.sql("select country from (select country,count(*) from global_temp.airport_data group by country order by count(*) desc) limit 1")

ans_panda=ans.toPandas()
ans_panda.to_csv(str(sys.argv[2]),index=False)
