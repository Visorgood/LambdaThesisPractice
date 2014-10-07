import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import redis.clients.jedis.Jedis;
import scala.Tuple2;

import com.google.common.collect.Lists;


public abstract class EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -4687571018192816211L;
	protected String schemaName;
	//protected Jedis jedis = new Jedis("localhost");
	private static final Pattern SPACE = Pattern.compile(" ");
	
	protected abstract void processEvent(GenericRecord record);
	
	public static EventProcessingStream getEventProcessingStreamByEventName(String eventName)
	{
		switch (eventName)
		{
			case "sms_received": return new SmsReceivedStream();
			case "sms_sent": return new SmsSentStream();
			case "app_install": return new AppInstallStream();
		}
		return null;
	}
	
	public void run(JavaStreamingContext jssc,	String zkQuorum, String group, Map<String, Integer> topicMap)
	{
		
		JavaPairReceiverInputDStream<String, String> messages =
	            KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
		
		
	    // input: tuple (null, JSON message)
		// output: JSON message
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>()
	    {
	      @Override
	      public String call(Tuple2<String, String> tuple2)
	      {

	    	  /*try
		  		{
		  			Schema schema = new Schema.Parser().parse(new File(schemaName + ".avsc"));
		  			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		  			InputStream in = new ByteArrayInputStream(tuple2._2().getBytes());
		  			GenericRecord record = datumReader.read(null, DecoderFactory.get().jsonDecoder(schema, in));
		  			processEvent(record);
		  		}
		  		catch (Exception e)
		  		{
		  			System.out.println("Exception raised!");
		  			System.out.println(e.getMessage());
		  		}*/
	    	  
	    	  
	        return tuple2._2();
	      }
	    });

	    // input: JSON message
		// output: distinct words from JSON message
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	      @Override
	      public Iterable<String> call(String x) {
	        return Lists.newArrayList(SPACE.split(x));
	      }
	    });

		// input: one word
		// intermediate output: word + 1
		// output: word + count
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s, 1);
	        }
	      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	      });
	    
	    
	    /*
	     * You'd have to do this by writing your own code to put the data into Redis on each iteration using an existing Java client, but it shouldn't be too bad. Your code would look like this:

			// stream is the DStream you want to save
			stream.foreachRDD { rdd =>
			 rdd.foreachPartition(iterator => { save values to Redis })
			}
			
			By calling foreachPartition, you'll get to see a whole partition of the results RDD at once and bulk-insert them into Redis.
			
			If your data is small, you could also collect() the RDD back to the driver program in foreachRDD and insert it from there.
	     */
	    
	    wordCounts.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>()
	    {
	        @Override
	        public Void call(JavaPairRDD<String, Integer> rdd) throws Exception
	        {
	        	rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>()
	        	{
	        		@Override
	        		public void call(Iterator<Tuple2<String, Integer>> partitionOfRecords)
	        		{
	        			System.out.println("new partition:");
	        			while(partitionOfRecords.hasNext())
	        			{
	        		         Object element = partitionOfRecords.next();
	        		         System.out.print(element + " ");
	        		         try
	        		         {
		        		         Schema schema = new Schema.Parser().parse(new File(schemaName + ".avsc"));
		        		         GenericRecord record = new GenericData.Record(schema);
		        		         record.put("id", 1);
		        		         record.put("userId", 2);
		        		         record.put("time", new Date().getTime());
		        		         record.put("contactHash", "");
		        		         record.put("msgLength", 0);
		     		  			 processEvent(record);
	        		         }
	        		         catch(Exception ex){}
	        		    }
	        		}
	        	});
	        	return null;
	        }
	    });

	    wordCounts.print();
	}
}
