# FileWatcher - Akka - Kafka

## Introduction:

The first part of this project is `FileWatcher - Akka -Kafka` which aims to design and implement an actor-model service using Akka that ingests logfile generated data in real time and delivers it via an event-based service called Kafka
 
Firstly, we create an actor system that is integrated with an event delivery system to enable notifications of real-time events, e.g., updates to the log file. 

Secondly, we create a delivery mechanism of the obtained events of interest to Spark, where further processing is done.

## Requirements:

Working AWS account, IntelliJ IDEA 2021.2.1(Community Edition), jdk 1.8.0_191, Scala 2.12.12, sbt 1.5.5, Akka 2.5.17

## Project Structure:

```
src
├── main
│   ├── resources
│   │   ├── application.conf
│   │   ├── logback.xml
│   │   └── logback-test.xml
│   └── scala
│       ├── Actors
│       │   ├── FileMonitor.scala
│       │   ├── FileProcessor.scala
│       │   ├── FileWatcher.scala
│       │   └── ThreadFileMonitor.scala
│       └── Utils
|       |   └── CreateLogger.scala
│       ├── ConsumerApp.scala
│       └── Driver.scala
└── test
    └── scala
        └── AkkaKafkaTestSuite.scala
```

## How to Run

**Step 1:**

Clone the repository and navigate inside the working directory
```
git clone https://github.com/gnzeleven/Cloud-Computing---Streaming-Data-Pipeline.git
cd Cloud-Computing---Streaming-Data-Pipeline
cd FileWatcher-Akka-Kafka
```

**Step 2:**

Run the Project using sbt

`run 1` and `run2` uses the logs generated in Instance1 and Instance2 respectively.


```
sbt clean compile 
sbt  
run 1  
run 2 
```
Run the following command to test the project

```
sbt test
```

## Implementation

This section's three primary components are briefly outlined below.

### java.nio.file

java.nio.file package provides a Watch Service API which is a file change notification API. It allows us to register a folder with the watch service. 
In the service, we can mention the events we are interested in when we register: file creation, file modification, and file deletion.


### AKKA Actor

Based on the changes observed, An actor is used to receive messages based on the changes and takes actions to handle them. 

Upon receiving a message, an actor may take one or more of the following actions:

- Execute some operations itself (such as performing calculations, persisting data, calling an external web service, and so on)
- Forward the message, or a derived message, to another actor
- Instantiate a new actor and forward the message to it
- Alternatively, the actor may choose to ignore the message entirely (i.e., it may choose inaction) if it deems it appropriate to do so.

To implement an actor, it is necessary to extend the akka.actor.Actor trait and implement the receive method. 


### Kafka

Apache Kafka is a distributed data storage designed for real-time input and processing of streaming data.

Kafka provides three main functions to its users:

- Streams of recordings can be published and subscribed to.
- Streams of records should be stored in the sequence in which they were generated.
- Real-time processing of record streams


### File Watcher - AKKA - Spark

**File Watcher**

- Actor System is created to monitor the folder for changes
- Based on the update made to the folder the file processing is to observe the changes are performed
- Possible actions are : File Creation, File Deletion and File Modification

**File Processor**

- Based on the update message received from the File Watcher. The changes are observed here.
- handleModify: retrieves the new lines added to the file, update the state and put them in Kafka stream
- handleCreate: update the state for the new file created
- handleDelete: delete the corresponding record from the current state
- Once the updates are detected, the changes are sent to the kafka queue.

**Kafka Queue**
- We use akka-stream-kafka to put the updated logs from Akka to Kafka streams that is configured in the AWS MSK service

Resource followed for creating MSK Cluster : [AWS MSK Cluster Documentation](https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html)

## EC2 Deployment

EC2 is used to deploy multiple instance of the log file generation program and they are run for some period of time producing and storing log messages into log file in a shared drive at EFS(Elastic File System). 

Akka Actor is also deployed in a EC2 to monitor the logs generated in the multiple EC2 instances.

