import org.apache.avro.generic.GenericRecord;

public class CallOutgoingBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = -8806319709455443417L;

	public CallOutgoingBolt()
	{
		schemaName = "call_outgoing";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}
