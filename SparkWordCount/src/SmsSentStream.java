import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;

public class SmsSentStream extends EventProcessingStream implements Serializable
{
	public SmsSentStream()
	{
		schemaName = "sms_received";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}