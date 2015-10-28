1. Copy the [data](http://stat-computing.org/dataexpo/2009/the-data.html) into HDFS. Name it e.g. `/user/cloudera/2008.csv`.
2. Compile jar file from the project with Maven: `mvn install`.
3. Run `hadoop target/flight3-0.0.1-SNAPSHOT.jar bigdata.Flight3 /user/cloudera/2008.csv /user/cloudera/flight3-results.csv`
4. Open the directory and inspect the file in it. It should contain the average delay times:
```
Summer	10.040898505269277
Winter	13.19495877145069
```
Which means winter delays are ~3 minutes longer on average.
