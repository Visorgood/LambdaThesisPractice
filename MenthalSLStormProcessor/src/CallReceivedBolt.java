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
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		String contactHash = (String)record.get("contactHash");
		long duration = (long)record.get("duration");
		eventAggregator.processCallReceived(userId, time, contactHash, time, duration);
	}
}
