import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
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
import backtype.storm.tuple.Tuple;

public abstract class EventProcessingBolt extends BaseRichBolt
{
	private static final long serialVersionUID = -8386633614335300892L;
	protected String schemaName;
	protected Jedis jedis;
	
	protected abstract void processEvent(GenericRecord record);
	
	public static EventProcessingBolt getEventProcessingBoltByEventName(String eventName)
	{
		switch (eventName)
		{
			case "sms_received": return new SmsReceivedBolt();
			case "sms_sent": return new SmsSentBolt();
			case "app_install": return new AppInstallBolt();
		}
		return null;
	}
	
	@Override
	public void execute(Tuple tuple)
	{
		try
		{
			Schema schema = new Schema.Parser().parse(new File(schemaName + ".avsc"));
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			InputStream in = new ByteArrayInputStream((byte[])tuple.getValue(0));
			GenericRecord record = datumReader.read(null, DecoderFactory.get().jsonDecoder(schema, in));
			processEvent(record);
		}
		catch (Exception e)
		{
			System.out.println("Exception raised!");
			System.out.println(e.getMessage());
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector)
	{
		jedis = new Jedis("localhost");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		//declarer.declare(new Fields(""));
	}
}
