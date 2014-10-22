import org.apache.avro.generic.GenericRecord;


public class CallReceivedStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 4009476339019869008L;

	public CallReceivedStream()
	{
		schemaName = "call_received";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		String contactHash = (String)record.get("contactHash");
		long duration = (long)record.get("duration");
		EventAggregator eventAggregator = new EventAggregator("localhost");
		eventAggregator.processCallReceived(userId, time, contactHash, time, duration);
	}
}
