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
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String contactHash = record.get("contactHash").toString();
		long duration = (long)record.get("durationInMillis");
		eventAggregator.processCallOutgoing(userId, time, contactHash, time, duration);
	}
}
