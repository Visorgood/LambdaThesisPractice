import org.apache.avro.generic.GenericRecord;

import redis.clients.jedis.Jedis;

public class SmsReceivedStream extends EventProcessingStream implements java.io.Serializable
{
	public SmsReceivedStream()
	{
		schemaName = "sms_received";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		Jedis jedis = new Jedis("localhost");
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}