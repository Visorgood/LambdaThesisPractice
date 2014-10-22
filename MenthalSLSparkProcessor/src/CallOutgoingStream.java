import org.apache.avro.generic.GenericRecord;


public class CallOutgoingStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -2503861431773136207L;

	public CallOutgoingStream()
	{
		schemaName = "call_outgoing";
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
		eventAggregator.processCallOutgoing(userId, time, contactHash, time, duration);
	}
}
