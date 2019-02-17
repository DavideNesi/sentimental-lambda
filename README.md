# sentimental-lambda
Lambda architecture code for tweets' sentiment analysis.

In collaboration with [Davide Nesi](https://github.com/DavideNesi).

<p align="center">
    <img width="343" height="193" src="https://github.com/rickie95/sentimental-lambda/blob/master/NotEssentialFiles/meme.png">
</p>

## Overview

This code is designed to work with Hadoop 2.9.2, Storm 1.2.2, Cassandra 3.11 and LingPipe.
### Architecture schema
<p align="center">
    <img width="100%" height="auto" src="https://github.com/rickie95/sentimental-lambda/blob/master/NotEssentialFiles/Architecture.png">
</p>

## Insights
### Hadoop code

Map (Filter tweets by keyword) -> Map (Assign a sentiment value to each tweet) -> Reduce (count each sentiment).

Usage:
Generate the `.jar` archive, then

    hadoop jar artifact-name.jar /input/dir/path /output/dir/path /path/to/lingpipe/classifier.lpc cassandra_ip:port [keyword]
    
If `keyword` is entered, only tweets that contains `keyword` will be counted.

### Storm code
1. HdfsSpout        (Read files from HDFS)
1. TweetRecorder    (Classifies tweets)
1. CassandaBolt     (Counts sentiments and save them in Cassandra DB)

Usage:
Generate the `.jar` archive, then start the topology:

    storm jar artifact-name.jar org.apache.storm.TweetTopology /path/to/lingpipe/classifier.lpc cassandra.db.ip.address:port [keyword]

If `keyword` is entered, only tweets that contains `keyword` will be counted.

### Cassandra details
Below the code for create the schemas in the database.

**DDL**
First, create the workspace: 

`CREATE KEYSPACE SentimentAnalysis WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };`
`USE SentimentAnalysis;`

Then proceed to create Hadoop tables

`CREATE TABLE positives ( timestmp timestamp, keyword varchar, count bigint, PRIMARY KEY (keyword, timestmp) ) WITH CLUSTERING ORDER BY (timestmp DESC);`

`CREATE TABLE negatives ( timestmp timestamp, keyword varchar, count bigint, PRIMARY KEY (keyword, timestmp) ) WITH CLUSTERING ORDER BY (timestmp DESC);`

`CREATE TABLE neutrals ( timestmp timestamp, keyword varchar, count bigint, PRIMARY KEY (keyword, timestmp) ) WITH CLUSTERING ORDER BY (timestmp DESC);`

Last, Storm tables:

`CREATE TABLE storm_queries ( timestmp timestamp, keyword varchar, positive bigint, negative bigint, neutral bigint,  PRIMARY KEY (keyword, timestmp) ) WITH CLUSTERING ORDER BY (timestmp DESC);`

Use CassandraDriver's methods for insert.

## Credits
* [LingPipe toolkit](http://alias-i.com/)
* [Nathan Marz](https://github.com/nathanmarz) - "Big Data", Manning.


