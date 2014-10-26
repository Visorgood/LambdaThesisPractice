LambdaThesisPractice
====================

Practical part of the LambdaThesis project

List of event types being processed:
<ul>
<li>app_install</li>
<li>app_session</li>
<li>screen_off</li>
<li>screen_unlock</li>
<li>sms_received</li>
<li>sms_sent</li>
<li>call_outgoing</li>
<li>call_received</li>
<li>call_missed</li>
</ul>

In the final Redis database, that plays the role of real-time views of the speed layer, we have following keys:<br />

app:$app_name:$user_id:sessions:* counters
app:$app_name:$user_id:total_time:* durations
user:$user_id:$app_name:app_usage:* counters
user:ALL_USERS_ID:$app_name:app_usage:* counters
user:$user_id:$app_name:app_starts:* durations
user:ALL_USERS_ID:$app_name:app_starts:* durations

user:$user_id:screen_lock:* counters

user:$user_id:$phone_hash:incoming_msg_count:* counters
user:ALL_USERS_ID:$phone_hash:incoming_msg_count:* counters
user:$user_id:$phone_hash:incoming_msg_length:* lengths
user:ALL_USERS_ID:$phone_hash:incoming_msg_length:* lengths

user:$user_id:$phone_hash:outgoing_msg_count:* counters
user:ALL_USERS_ID:$phone_hash:outgoing_msg_count:* counters
user:$user_id:$phone_hash:outgoing_msg_length:* lengths
user:ALL_USERS_ID:$phone_hash:outgoing_msg_length:* lengths


<h3>EventGenerator</h3>

Generates random events of different types, prints them out to the console, and sends them to Kafka.<br />
Those events can be then processed by Storm and Spark.

<h3>KafkaAvroProducer</h3>

Takes events from avro files, and sends them to kafka.<br />
We don't use this project any more.

<h3>MenthalSLStormProcessor</h3>

Our main project, that contains data processing using Storm.

<h3>MenthalSLSparkProcessor</h3>

Our second main project, that contains data processing using Spark.

<h3>ParquetAvroConverter</h3>

Takes parquet file, parses it using SparkPlayingField library, and saves data to avro files.<br />
We don't use this project any more.

<h3>SparkWordCount</h3>

Simple test project to try Spark via word counting.

<h3>TestJedis</h3>

Test project to try playing with Redis and Jedis.<br />
It is already quite obsolete, and can be easily deleted.

<h3>TestStorm</h3>

Old Storm project, that is now recreated as MenthalSLStormProcessor.<br />
It is now obsolete, and can be deleted.
