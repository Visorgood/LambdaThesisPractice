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


<h1>EventGenerator</h1>

Generates random events of different types, prints them out to the console, and sends them to Kafka.
Those events can be then processed by Storm and Spark.
