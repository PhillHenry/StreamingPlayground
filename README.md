# StreamingPlayground
Experiments with various streaming technologies.

The code in thie repo will create Spark, Kafka etc cluster within Docker all running on a single laptop.
For this, it uses [Dreadnought](https://github.com/PhillHenry/dreadnought).

# Example
`SparkStructuredStreamingMain` will start a Kafka cluster of 3 brokers using the KRaft protocol
(ergo, no need for Zookeeper), a local S3 bucket (using Minio) and a Spark master and worker.
It will then send messages to Kafka that are consumed via Spark Structured Streaming
and then written to a Dockerised S3 bucket. It demonstrates that 
["Enable no-data micro batches for more eager streaming state clean up"](https://issues.apache.org/jira/browse/SPARK-24156}) 
still appears to be an issue.

# Caveats
- The Docker containers may make calls to the host computer 
(for instance, if a Spark driver runs on the host, the workers need to talk to it).
This means you'll have to open the relevant ports in your firewall.
- The code needs tidying. It's very much proof-of-concept at the moment.
- Dreadnought does not automatically pull the Docker images for you if they're not on your machine already. 
At the moment, you'll have to use Docker to do that. However, you only need do this once. 
