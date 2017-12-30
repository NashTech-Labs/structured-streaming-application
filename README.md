# Structured Streaming Application

It is a reference application (which we will constantly improve) showing how to easily leverage and integrate [Spark Structured Streaming](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html),
[Apache Cassandra](http://cassandra.apache.org), and [Apache Kafka](http://kafka.apache.org) for streaming computations.
  
## Sample Use Case
We need to calculate streaming Word Count.

### Clone the repo

    git clone https://github.com/knoldus/structured-streaming-application.git
    cd structured-streaming-application

### Build the code 
If this is your first time running SBT, you will be downloading the internet.

    cd structured-streaming-application
    sbt clean compile

### Setup - 4 Steps
1.[Download the latest Cassandra](http://cassandra.apache.org/download/) and open the compressed file.

2.Start Cassandra - you may need to prepend with sudo, or chown /var/lib/cassandra. On the command line:

    ./apache-cassandra-{version}/bin/cassandra -f

3.[Download Kafka 0.10.2.1](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.2.1/kafka_2.11-0.10.2.1.tgz)

4.Start the Kafka Server

    cd kafka_2.11-0.10.2.1
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties

### Run
#### From Command Line
1.Start `Structured Streaming Application`

    cd /path/to/structured-streaming-application
    sbt run
    Multiple main classes detected, select one to run:
        
         [1] knolx.DataStreamer
         [2] knolx.StructuredStreamingWordCount
        
        Enter number: 2

2.Start the Kafka data feed
In a second shell run:

    cd /path/to/structured-streaming-application
    sbt run
    Multiple main classes detected, select one to run:
    
     [1] knolx.DataStreamer
     [2] knolx.StructuredStreamingWordCount
    
    Enter number: 1

After a few seconds you should see data by entering this in the cqlsh shell:

    cqlsh> select * from wordcount;

This confirms that data from the app has published to Kafka, and the data is
streaming from Spark to Cassandra.