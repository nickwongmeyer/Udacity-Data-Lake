# Udacity-Data-Lake
Setting up Data Lake via Spark/Hadoop in AWS

## Overview

The aim of this project is to move the data from S3 JSON log to Data Lake via spark programming to handle the metadata. loading the data back to my own S£ bucket as a set of dimensional tables, which is one of the essential procedure to build ETL pipeline under the duty of data engineer. 

## Configuration 

AWS credentials must be filled under the ```dl.cfg``` file  access them via the Secret Access Key ID and Secret access Key, ensuring that the user has full adminaccess under 'AmazonRedshiftFullAccess' and 'AmazonS3ReadOnlyAccess'.

As soon as the information of dl.cfg has been completed, run the ```etl.py``` via terminal as ```python3 etl.py```.

# Schema 

A star schema has been used which is very similar to the previous project under data warehouse. Fact table is structured as ```songplays```, whereas ```artists```, ```songs```, ```time``` and ```users``` are regarded as dimensional tables. 

**Fact Table**

- records in log data associated with song plays i.e. records with page ```NextSong```
```songplay_id```, ```start_time```, ```user_id```, ```level```, ```song_id```, ```artist_id```, ```session_id```, ```location```, ```user_agent```

**Dimension Tables**

- users - users in the app
```user_id```, ```first_name```, ```last_name```, ```gender```, ```level```

- songs - songs in music database
```song_id```, ```title```, ```artist_id```, ```year```, ```duration```

- artists - artists in music database
```artist_id```, ```name```, ```location```, ```lattitude```, ```longitude```

- time - ```timestamps``` of records in songplays broken down into specific units
```start_time```, ```hour```, ```day```, ```week```, ```month```, ```year```, ```weekday```


