
##Lab 6 : Introductio to Apache Spark and Spark SQL

MIDS W205 Storing and Retrieving Data

*Submitted by : Andrea Soto*

##Submission 1

<p>There are two issues with the tuple. First, the value is a long string and is not separated into columns. And second, the key is still present in the value as the first term.</p>

Both issues were resolved by adjusting the lambda function as follows:

````
narcoticsCrimes.map(lambda x: (x.split(",")[0], x.split(",")[1:]))
````

The python script with the steps for the first submission in called script1.py. This file prints out the first 10 lines of the sorted and unsorted RDDs. The output was saved in the file output1.txt

##Submission 2

There are **40001** rows in the DataFrame (excluding the first row with headers) and **3943** Ebay entries. 

The python script with the steps for this submission is called script2.py. This output from this script was saved in the file output2.txt
