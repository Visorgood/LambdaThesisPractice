import java.util.Map;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public final class Main
{
	private static String[] eventNames = new String[] {
		"app_install", "app_session" , "screen_off",
		"screen_unlock", "sms_received", "sms_sent",
		"call_outgoing", "call_received", "call_missed"};

  private Main() { }

  public static void main(String[] args) 
  {

    SparkConf sparkConf = new SparkConf().setAppName("MenthalSLSparkProcessor");
    sparkConf.setMaster(String.format("local[%d]", eventNames.length + 1));
    // Create the context with a 1 second batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
    
    // the number of threads the kafka consumer should use
    int numThreads = 1;
    // a list of one or more zookeeper servers that make quorum
    String zkQuorum = "localhost:5181";
    // the name of kafka consumer group
    String group = "test-consumer-group";
    
	for (String eventName : eventNames)
	{
		// kafka topic to consume from
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    topicMap.put(eventName, numThreads);
	    
		EventProcessingStream stream = EventProcessingStream.getEventProcessingStreamByEventName(eventName);
		stream.run(jssc, zkQuorum, group, topicMap);
	}
	
	jssc.start();
	jssc.awaitTermination();
  }
}