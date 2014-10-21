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
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		String contactHash = (String)record.get("contactHash");
		long duration = (long)record.get("duration");
		eventAggregator.processCallOutgoing(userId, time, contactHash, time, duration);
	}
}
