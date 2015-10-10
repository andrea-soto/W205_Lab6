
from pyspark import SparkContext

sc = SparkContext("local", "weblog app")

crimedata =sc.textFile("file:///data/asoto/lab6/Crimes_-_2001_to_present.csv")

noHeaderCrimedata = crimedata.zipWithIndex().filter(lambda (row,index): index > 0).keys()
narcoticsCrimes = noHeaderCrimedata.filter(lambda x:"NARCOTICS" in x)

narcoticsCrimeTuples = narcoticsCrimes.map(lambda x: (x.split(",")[0], x.split(",")[1:]))

sorted=narcoticsCrimeTuples.sortByKey()

print "\nFirst 10 rows of unsorted RDD:"
x = narcoticsCrimeTuples.take(10)
for i in range(10):
    print "Line_"+str(i+1)+"\t"+ str(x[i])+ '\n'

print "\nFirst 10 rows of sorted RDD:"
x = sorted.take(10)
for i in range(10):
    print "Line_"+str(i+1)+"\t"+ str(x[i])+ '\n'

print "\nThis file was created by running the following command:"
print "spark-submit script1.py > output1.txt"

print "\n"
