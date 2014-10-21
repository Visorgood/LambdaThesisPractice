import org.apache.avro.generic.GenericRecord;

public class CallReceivedBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = -7580395188906387752L;

	public CallReceivedBolt()
	{
		schemaName = "call_received";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}
