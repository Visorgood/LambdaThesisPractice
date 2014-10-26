LambdaThesisPractice
====================

Practical part of the LambdaThesis project

<div>
List of event types being processed:
    app_install
    app_session
    screen_off
    screen_unlock
    sms_received
    sms_sent
    call_outgoing
    call_received
    call_missed
</div>

<h3>EventGenerator</h3>

Generates random events of different types, prints them out to the console, and sends them to Kafka.
Those events can be then processed by Storm and Spark.

<h3>KafkaAvroProducer</h3>

Takes events from avro files, and sends them to kafka.
We don't use this project any more.

<h3>MenthalSLStormProcessor</h3>

Our main project, that contains data processing using Storm.

<h3>MenthalSLSparkProcessor</h3>

Our second main project, that contains data processing using Spark.

<h3>ParquetAvroConverter</h3>

Takes parquet file, parses it using SparkPlayingField library, and saves data to avro files.
We don't use this project any more.

<h3>SparkWordCount</h3>

Simple test project to try Spark via word counting.

<h3>TestJedis</h3>

Test project to try playing with Redis and Jedis.
It is already quite obsolete, and can be easily deleted.

<h3>TestStorm</h3>

Old Storm project, that is now recreated as MenthalSLStormProcessor.
It is now obsolete, and can be deleted.
