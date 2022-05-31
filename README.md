# exam-spark-kafka

The solution for challenge 1 form the Scala academy exam.

## What you need

* Java 11
* Scala 2.12.15
* Sbt 1.6.2
* Kafka 3.2
* Spark 3.2.1

## How to run

* Before running the application you must create input topic. You can create topics by typing the command  
```text
*kafka-topics.sh --create --topic <topic name> --bootstrap-server localhost:9092
``` 
in the linux command shell which is opened in a folder with the Kafka
* You must also run hdfs and yarn on your machine. [Here](https://hadoop.apache.org/docs/r3.3.2/hadoop-project-dist/hadoop-common/SingleCluster.html), you can find installation manual for them.
* In order to run the application:
  1. Build jar using command `sbt package`
  2. In the folder with spark enter the command  
    ```text
    ./bin/spark-submit --class SparkKafkaApp --master yarn --deploy-mode cluster [options] <path to jar> [app options]
    ```
* List of parameters should contain one element with the name of input topic. If it is not given, default name "input" will be used. 
* The application connects to hdfs at the address `localhost:9000` and writes to folder `/output`


## How to run tests
* In order to run tests enter the command `sbt test `

## FAQ
* The application has problems to write results to parquet file on hdfs. I did not have time to debug this. However, the application generates the data correctly. One can check that by uncommenting 'For testing' part and commenting 'Writing to parquet on hdfs' part
* Remember to run Zookeeper and Kafka server before creating topics.
* Remember to give proper permissions to your hdfs drive. You can do this by entering the command `sudo -u <user> hdfs dfs -chmod 775 /output`. More information about this problem you can find [here](https://community.cloudera.com/t5/Support-Questions/Permission-denied-user-mapred-access-WRITE-inode-quot-quot/m-p/16318)
