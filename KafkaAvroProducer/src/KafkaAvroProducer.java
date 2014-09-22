/*
 * required libraries:
 * 	  kafka_2.9.2-0.8.1.1.jar
 * 	  log4j-1.2.15.jar
 * 	  metrics-core-2.2.0.jar
 * 	  scala-library-2.9.2.jar
 * 	  slf4j-api-1.7.2.jar
 *    
 */

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaAvroProducer
{
	static String[] eventNames = new String[] { "sms_received", "sms_sent", "app_install" };
	
	public static void main(String[] args) throws IOException
	{	
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		//sendRandomEvents(producer);
		sendAvroEvents(producer);
		producer.close();
	}
	
	static void sendRandomEvents(Producer<String, String> producer) throws IOException
	{
		Random r = new Random();
		for (String eventName : eventNames)
		{
			Schema schema = new Schema.Parser().parse(new File(eventName + ".avsc"));
			GenericRecord record;
			for (long i = 0; i < 10; ++i)
			{
				record = new GenericData.Record(schema);
				record.put("id", r.nextLong() % 100000);
				record.put("userId", r.nextLong() % 100000);
				record.put("time", new Date().getTime());
				//record.put("contactHash", "");
				//record.put("msgLength", 0);
				producer.send(new KeyedMessage<String, String>(eventName, record.toString()));
			}
		}
	}
	
	static void sendAvroEvents(Producer<String, String> producer) throws IOException
	{
		for (String eventName : eventNames)
		{
			Schema schema = new Schema.Parser().parse(new File(eventName + ".avsc"));
			File dir = new File("/home/user/LambdaThesisPractice/parquet_avro_binary/" + eventName);
			
			for (File file : dir.listFiles())
			{
				if (!file.getName().endsWith(".avro"))
					continue;
				
			    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
			    
			    GenericRecord record = null;
			    while (dataFileReader.hasNext())
			    {
			    	record = dataFileReader.next(record);
			    	System.out.println(record.toString());
					producer.send(new KeyedMessage<String, String>(eventName, record.toString()));
			    }
			    
			    dataFileReader.close();
			}
		}
	}
}