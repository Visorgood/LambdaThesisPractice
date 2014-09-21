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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaAvroProducer
{
	public static void main(String[] args) throws IOException
	{	
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		sendRandomEvents(producer);
		//sendAvroEvents(producer);
		producer.close();
	}
	
	static void sendRandomEvents(Producer<String, String> producer) throws IOException
	{
		Random r = new Random();
		Schema schema = new Schema.Parser().parse(new File("/home/user/sms_received.avsc"));
		GenericRecord record;
		for (long i = 0; i < 10; ++i)
		{
			record = new GenericData.Record(schema);
			record.put("id", i);
			record.put("userId", r.nextLong());
			record.put("time", new Date().getTime());
			record.put("contactHash", "");
			record.put("msgLength", 0);
			producer.send(new KeyedMessage<String, String>("test", record.toString()));
		}
	}
	
	static void sendAvroEvents(Producer<String, String> producer) throws IOException
	{
		//File file = new File("/home/user/some_avro_file");
	    //DatumReader<String> userDatumReader = new SpecificDatumReader<String>(String.class);
	    //DataFileReader<String> dataFileReader = new DataFileReader<String>(file, userDatumReader);
	    
	    //SmsReceived smsReceived = null;
	    //while (dataFileReader.hasNext())
	    //{
	    	//smsReceived = dataFileReader.next(smsReceived);
			//producer.send(new KeyedMessage<String, String>("test", message));
	    //}
	    
	    //dataFileReader.close();
	}
}