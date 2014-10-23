
import java.io.File;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Time;

import scala.Tuple2;


public abstract class EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -4687571018192816211L;
	protected String schemaName;
	
	protected abstract void processEvent(GenericRecord record, EventAggregator eventAggregator);
	
	public static EventProcessingStream getEventProcessingStreamByEventName(String eventName)
	{
		switch (eventName)
		{
			case "app_install": return new AppInstallStream();
			case "app_session": return new AppSessionStream();
			case "screen_off": return new ScreenOffStream();
			case "screen_unlock": return new ScreenUnlockStream();
			case "sms_received": return new SmsReceivedStream();
			case "sms_sent": return new SmsSentStream();
			case "call_outgoing": return new CallOutgoingStream();
			case "call_received": return new CallReceivedStream();
			case "call_missed": return new CallMissedStream();
		}
		return null;
	}
	
	public void run(JavaStreamingContext jssc,	String zkQuorum, String group, Map<String, Integer> topicMap)
	{
		
		JavaPairReceiverInputDStream<String, String> stream =
	            KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
		
		
		stream.foreachRDD(new Function2<JavaPairRDD<String, String>, Time, Void>()
	    {
	        @Override
	        public Void call(JavaPairRDD<String, String> rdd, Time time) throws Exception
	        {
	        	rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>()
	        	{
	        		@Override
	        		public void call(Iterator<Tuple2<String, String>> partitionOfRecords)
	        		{
	        			System.out.println("new partition:");
	        			
	        			// Why eventAggregator is here: http://blog.csdn.net/luyee2010/article/details/39291163
	        			EventAggregator eventAggregator = new EventAggregator("localhost");
	        			
	        			while(partitionOfRecords.hasNext())
	        			{
	        				Tuple2<String, String> element = partitionOfRecords.next();
	        		        System.out.print(element + " ");
	        		        try
	        		        {
		        		         Schema schema = new Schema.Parser().parse(new File(schemaName + ".avsc"));
		        		         DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
	        					 GenericRecord record = datumReader.read(null, DecoderFactory.get().jsonDecoder(schema, element._2()));
	        					 processEvent(record, eventAggregator);
	        		         }
	        		         catch(Exception ex)
	        		         {
	        		        	System.out.println("Exception raised!");
	        		 			System.out.println(ex.getMessage());
	        		         }
	        		    }
	        		}
	        	});
	        	return null;
	        }
	    });
	}
}
