LambdaThesisPractice
====================

Practical part of the LambdaThesis project

<h4>EventGenerator</h4>

Generates random events of different types, prints them out to the console, and sends them to Kafka.<br />
Those events can be then processed by Storm and Spark.

<h4>KafkaAvroProducer</h4>

Takes events from avro files, and sends them to kafka.<br />
We don't use this project any more.

<h4>MenthalSLStormProcessor</h4>

Our main project, that contains data processing using Storm.

<h4>MenthalSLSparkProcessor</h4>

Our second main project, that contains data processing using Spark.

<h4>ParquetAvroConverter</h4>

Takes parquet file, parses it using SparkPlayingField library, and saves data to avro files.<br />
We don't use this project any more.

<h4>SparkWordCount</h4>

Simple test project to try Spark via word counting.

<h4>TestJedis</h4>

Test project to try playing with Redis and Jedis.<br />
It is already quite obsolete, and can be easily deleted.

<h4>TestStorm</h4>

Old Storm project, that is now recreated as MenthalSLStormProcessor.<br />
It is now obsolete, and can be deleted.

<h4>List of events that we process for this moment</h4>

<ul>
<li>app_install</li>
<li>app_session</li>
<li>call_missed</li>
<li>call_outgoing</li>
<li>call_received</li>
<li>dreaming_started</li>
<li>dreaming_stopped</li>
<li>phone_shutdown</li>
<li>screen_off</li>
<li>screen_on</li>
<li>screen_unlock</li>
<li>sms_received</li>
<li>sms_sent</li>
<li>window_state_changed</li>
</ul>

<h4>Keys in Redis</h4>

In the final Redis database, that plays the role of real-time views of the speed layer, we have following keys.<br />
Each key is associated with the list, that has two elements. The first one is the date-time of the beginning of counting. The second one is the value itself.
<ul>
<li>app:$app_name:$user_id:sessions:* counters</li>
<li>app:$app_name:$user_id:total_time:* durations</li>
<li>user:$user_id:$app_name:app_usage:* counters</li>
<li>user:allUsers:$app_name:app_usage:* counters</li>
<li>user:$user_id:$app_name:app_starts:* durations</li>
<li>user:allUsers:$app_name:app_starts:* durations</li>

<li>user:$user_id:screen_lock:* counters

<li>user:$user_id:$phone_hash:incoming_msg_count:* counters</li>
<li>user:allUsers:$phone_hash:incoming_msg_count:* counters</li>
<li>user:$user_id:$phone_hash:incoming_msg_length:* lengths</li>
<li>user:allUsers:$phone_hash:incoming_msg_length:* lengths</li>

<li>user:$user_id:$phone_hash:outgoing_msg_count:* counters</li>
<li>user:allUsers:$phone_hash:outgoing_msg_count:* counters</li>
<li>user:$user_id:$phone_hash:outgoing_msg_length:* lengths</li>
<li>user:allUsers:$phone_hash:outgoing_msg_length:* lengths</li>

<li>user:$user_id:$phone_hash:outgoing_call_count:* counters</li>
<li>user:allUsers:$phone_hash:outgoing_call_count:* counters</li>
<li>user:$user_id:$phone_hash:outgoing_call_duration:* durations</li>
<li>user:allUsers:$phone_hash:outgoing_call_duration:* durations</li>

<li>user:$user_id:$phone_hash:incoming_call_count:* counters</li>
<li>user:allUsers:$phone_hash:incoming_call_count:* counters</li>
<li>user:$user_id:$phone_hash:incoming_call_duration:* durations</li>
<li>user:allUsers:$phone_hash:incoming_call_duration:* durations</li>

<li>user:$user_id:$phone_hash:missed_call_count:* counters</li>
<li>user:allUsers:$phone_hash:missed_call_count:* counters</li>
</ul>

Each time * means, that there are 4 counters with that base name and additional part, for example<br />
<br />
app:$app_name:$user_id:sessions:* counters - means that there are exactly the following keys<br />
<ul>
<li>app:$app_name:$user_id:sessions:count:hourly</li>
<li>app:$app_name:$user_id:sessions:count:daily</li>
<li>app:$app_name:$user_id:sessions:count:weekly</li>
<li>app:$app_name:$user_id:sessions:count:monthly</li>
</ul>
<br />
app:$app_name:$user_id:total_time:* durations - means that there are
<ul>
<li>app:$app_name:$user_id:total_time:duration:hourly</li>
<li>app:$app_name:$user_id:total_time:duration:daily</li>
<li>app:$app_name:$user_id:total_time:duration:weekly</li>
<li>app:$app_name:$user_id:total_time:duration:monthly</li>
</ul>
user:$user_id:$phone_hash:incoming_msg_length:* lengths - means that there are
<ul>
<li>user:$user_id:$phone_hash:incoming_msg_length:length:hourly</li>
<li>user:$user_id:$phone_hash:incoming_msg_length:length:daily</li>
<li>user:$user_id:$phone_hash:incoming_msg_length:length:weekly</li>
<li>user:$user_id:$phone_hash:incoming_msg_length:length:monthly</li>
</ul>
