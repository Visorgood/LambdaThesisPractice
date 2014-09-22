import org.apache.avro.generic.GenericRecord;

public class SmsSentBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = -7020024151853656410L;

	public SmsSentBolt()
	{
		schemaName = "sms_sent";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}