LambdaThesisPractice
====================

Practical part of the LambdaThesis project

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


<h2>EventGenerator</h2>

Generates random events of different types, prints them out to the console, and sends them to Kafka.
Those events can be then processed by Storm and Spark.

<h2>KafkaAvroProducer</h2>

Takes events form avro files, and sends them to kafka.
We don't use this prject any more.

<h2>MenthalSLStormProcessor</h2>

Our main project, that contains data processing using Storm.

<h2>MenthalSLSparkProcessor</h2>

Our second main project, that contains data processing using Spark.

<h2>ParquetAvroConverter</h2>

Takes parquet file, parses it using SparkPlayingField library, and saves data to avro files.
We don't use this prject any more.

<h2>SparkWordCount</h2>

Simple test project to try Spark via word counting.

<h2>TestJedis</h2>

Test project to try playing with Redis and Jedis.
It is already quite obsolete, and can be easily deleted.

<h2>TestStorm</h2>

Old Storm project, that is now recreated as MenthalSLStormProcessor.
It is now obsolete, and can be deleted.
