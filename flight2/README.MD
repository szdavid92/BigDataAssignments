##Which airport did the largest number of planes depart?

###Hive solution

Copy the [data](http://stat-computing.org/dataexpo/2009/the-data.html) into HDFS. Name it e.g. `/user/cloudera/2008.csv`.

Create a temporary dataset from the csv in Hive. Run `hive` and create a table:

```sql
create table temp_f2008 (col_value STRING);
```

and load the dataset into it:

```sql
LOAD DATA INPATH '/user/cloudera/2008.csv' OVERWRITE INTO TABLE temp_f2008;
```

We can check if it worked by printing out some of the rows
```sql
select * from temp_f2008 limit 5;
```

Now we create a table the holds solely the data needed for the calculation, ie. DepTime and Origin
```sql
create table departures(deptime STRING, origin STRING);
``` 

Then extract the corresponding values from `temp_f2008` with regexes:

```sql
insert overwrite table departures  
SELECT  
  regexp_extract(col_value, '^(?:([^,]*)\,?){5}', 1) deptime,  
  regexp_extract(col_value, '^(?:([^,]*)\,?){17}', 1) origin
from temp_f2008;
```

This should take a while. Once again, we can check if it worked.

The only thing left to do is to count the departure times in each of the airports:
```sql
select count(deptime) as count, Origin from departures group by Origin sort by count desc;
```

The first row contains the answer, that is Hartsfield Jackson Atlanta International Airport with 414513 departures.

Note: poor csv header row made it into the dataset, but it doesn't mess up the result in this particular case.



