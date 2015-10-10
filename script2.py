
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc = SparkContext("local", "weblog app")
sqlContext = SQLContext(sc)

lines = sc.textFile('file:///data/asoto/lab6/dwn_data/data/weblog_lab.csv')
noHeaderLines = lines.zipWithIndex().filter(lambda (row,index): index > 0).keys()

parts = noHeaderLines.map(lambda l: l.split('\t'))
Web_Session_Log = parts.map(lambda p: (p[0], p[1],p[2],p[3],p[4]))

schemaString = 'DATETIME USERID SESSIONID PRODUCTID REFERERURL'
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schemaWebData = sqlContext.createDataFrame(Web_Session_Log, schema)
schemaWebData.registerTempTable('Web_Session_Log')

print "\nThis file was created by running the following command:"
print "spark-submit script2.py > output2.txt"

print "\nNumber of DataFrame rows:"
sqlContext.sql('SELECT count(*) FROM Web_Session_Log').show()

print "\nNumber of Ebay entries:"
sqlContext.sql('SELECT count(*) FROM Web_Session_Log WHERE REFERERURL="http://www.ebay.com"').show()

print "\n20 rows of table:\n"
sqlContext.sql('SELECT * FROM Web_Session_Log').show()

print "\n"
