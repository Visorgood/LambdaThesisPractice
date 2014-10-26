import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class EventGenerator {
	private static int NUMBER_OF_EVENTS = 10;          // the number of events to generate
	private static int MIN_INTERVAL = 100;             // minimal interval in milliseconds between generation of events
	private static int MAX_ADDITIONAL_INTERVAL = 200;  // additional interval in milliseconds
	                                                   // total interval between generation of events is in the range [MIN_INTERVAL; MIN_INTERVAL + MAX_ADDITIONAL_INTERVAL)
	
	private static long NUMBER_OF_USERS = 100;         // the number of possible users (user ids)
	private static int NUMBER_OF_APPS = 100;           // the number of possible apps (app names)
	private static int NUMBER_OF_WINDOWS = 5;          // the number of possible window titles within app
	private static int MAX_MSG_LENGTH = 200;           // the maximal length of the message
	private static long MAX_DURATION = 15 * 60 * 1000; // the maximal duration of the event, that has the field "duration" (equals to 15 minutes)
	
	private static Random random = new Random();
	private static String[] eventNames = new String[] {
		"app_install", "app_session" , "call_missed", "call_outgoing",
		"call_received", "dreaming_started", "dreaming_stopped", "phone_shutdown",
		"screen_off", "screen_on", "screen_unlock", "sms_received",
		"sms_sent", "window_state_changed"};
	
	public static void main(String[] args) throws IOException, InterruptedException {
		// parse arguments to use the specified number of events to generate
		if (args != null && args.length > 0) {
			NUMBER_OF_EVENTS = Integer.parseInt(args[0]);
		}
			
		// initialize kafka producer
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		// generate events
		for (int i = 0; i < NUMBER_OF_EVENTS; ++i) {
			// choose randomly what type of event to generate
			int k = random.nextInt(eventNames.length);
			String eventName = eventNames[k];
			// generate, print out, and send event to kafka
			GenericRecord record = generateEvent(eventName);
			System.out.println(record);
			producer.send(new KeyedMessage<String, String>(eventName, record.toString()));
			Thread.sleep(MIN_INTERVAL + random.nextInt(MAX_ADDITIONAL_INTERVAL));
		}
		
		producer.close();
	}
	
	// generate random event by its string name
	private static GenericRecord generateEvent(String eventName) throws IOException {
		Schema schema = new Schema.Parser().parse(new File(eventName + ".avsc"));
		GenericRecord record = new GenericData.Record(schema);
		switch (eventName) {
			case "app_install": generateAppInstallEvent(record); break;
			case "app_session": generateAppSessionEvent(record); break;
			case "call_missed": generateCallMissedEvent(record); break;
			case "call_outgoing": generateCallOutgoingEvent(record); break;
			case "call_received": generateCallReceivedEvent(record); break;
			case "dreaming_started": generateDreamingStarted(record); break;
			case "dreaming_stopped": generateDreamingStopped(record); break;
			case "phone_shutdown": generatePhoneShutdown(record); break;
			case "screen_off": generateScreenOffEvent(record); break;
			case "screen_on": generateScreenOnEvent(record); break;
			case "screen_unlock": generateScreenUnlockEvent(record); break;
			case "sms_received": generateSmsReceivedEvent(record); break;
			case "sms_sent": generateSmsSentEvent(record); break;
			case "window_state_changed": generateWindowStateChanged(record); break;
		}
		return record;
	}
	
	private static void generateAppInstallEvent(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
		record.put("appName", generateAppName());
		record.put("packageName", "");
	}
	
	private static void generateAppSessionEvent(GenericRecord record) {
		record.put("userId", generateUserId());
		record.put("time", generateTime());
		record.put("duration", generateDuration());
		record.put("appName", generateAppName());
		record.put("packageName", "");
	}
	
	private static void generateCallMissedEvent(GenericRecord record) {
		long time = generateTime();
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", time);
		record.put("contactHash", generateContactHash());
		record.put("timestamp", time);
	}
	
	private static void generateCallOutgoingEvent(GenericRecord record) {
		long time = generateTime();
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", time);
		record.put("contactHash", generateContactHash());
		record.put("startTimestamp", time);
		record.put("durationInMillis", generateDuration());
	}
	
	private static void generateCallReceivedEvent(GenericRecord record) {
		long time = generateTime();
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", time);
		record.put("contactHash", generateContactHash());
		record.put("startTimestamp", time);
		record.put("durationInMillis", generateDuration());
	}
	
	private static void generateDreamingStarted(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
	}
	
	private static void generateDreamingStopped(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
	}
	
	private static void generatePhoneShutdown(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
	}
	
	private static void generateScreenOffEvent(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
	}
	
	private static void generateScreenOnEvent(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
	}
	
	private static void generateScreenUnlockEvent(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
	}
	
	private static void generateSmsReceivedEvent(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
		record.put("contactHash", generateContactHash());
		record.put("msgLength", generateMsgLength());
	}
	
	private static void generateSmsSentEvent(GenericRecord record) {
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
		record.put("contactHash", generateContactHash());
		record.put("msgLength", generateMsgLength());
	}
	
	private static void generateWindowStateChanged(GenericRecord record) {
		String appName = generateAppName();
		record.put("id", 1L);
		record.put("userId", generateUserId());
		record.put("time", generateTime());
		record.put("appName", appName);
		record.put("packageName", "");
		record.put("windowTitle", appName + ":" + generateWindowTitle());
	}
	
	// generate user id in the range [1; NUMBER_OF_USERS]
	private static long generateUserId() {
		return genLong() % NUMBER_OF_USERS + 1L;
	}
	
	// generate current time in unix format
	private static long generateTime() {
		return new DateTime().getMillis();
	}

	// generate duration in the range [0; MAX_DURATION)
	private static long generateDuration() {
		return genLong() % MAX_DURATION;
	}
	
	// generate app name in the range [1; NUMBER_OF_APPS]
	private static String generateAppName() {
		return "app" + Integer.toString(genInt() % NUMBER_OF_APPS + 1);
	}
	
	// generate "contact hash" in the range [1; NUMBER_OF_USERS]
	private static String generateContactHash() {
		return "contactHash" + Long.toString(genLong() % NUMBER_OF_USERS + 1L);
	}
	
	// generate window title in the range [1; NUMBER_OF_WINDOWS]
	private static String generateWindowTitle() {
		return "windowTitle" + Integer.toString(genInt() % NUMBER_OF_WINDOWS + 1);
	}
	
	// generate message length in the range [1; MAX_MSG_LENGTH]
	private static int generateMsgLength() {
		return genInt() % MAX_MSG_LENGTH + 1;
	}
	
	// generate positive long value
	private static long genLong() {
		return Math.abs(random.nextLong());
	}
	
	// generate positive int value
	private static int genInt() {
		return Math.abs(random.nextInt());
	}
}
