import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class EventProcessingBolt extends BaseRichBolt
{
	private static final long serialVersionUID = -8386633614335300892L;
	private static Jedis jedis;
	
	@Override
	public void execute(Tuple tuple)
	{
		try
		{
			Schema schema = new Schema.Parser().parse(new File("/home/user/sms_received.avsc"));
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			InputStream in = new ByteArrayInputStream((byte[])tuple.getValue(0));
			GenericRecord record = datumReader.read(null, DecoderFactory.get().jsonDecoder(schema, in));
			jedis.hset("sms_received", record.get("id").toString(), record.toString());
			System.out.println(record);
		}
		catch (Exception e)
		{
			System.out.println("Exception raised!");
			System.out.println(e.getMessage());
		}
	}
	
	public SmsReceived toSmsReceived(byte[] smsReceivedBytes)
	{
		SmsReceived smsReceived = new SmsReceived();
		smsReceived.setId(new BigInteger(Arrays.copyOfRange(smsReceivedBytes, 0, 8)).longValue());
		smsReceived.setUserId(new BigInteger(Arrays.copyOfRange(smsReceivedBytes, 8, 16)).longValue());
		smsReceived.setTime(new BigInteger(Arrays.copyOfRange(smsReceivedBytes, 16, 24)).longValue());
		return smsReceived;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector)
	{
		jedis = new Jedis("localhost");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		//declarer.declare(new Fields("word"));
	}
}
