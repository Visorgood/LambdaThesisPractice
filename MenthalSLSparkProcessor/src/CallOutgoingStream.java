import org.apache.avro.generic.GenericRecord;


public class CallOutgoingStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -2503861431773136207L;

	public CallOutgoingStream()
	{
		schemaName = "call_outgoing";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String contactHash = record.get("contactHash").toString();
		long duration = (long)record.get("duration");
		eventAggregator.processCallOutgoing(userId, time, contactHash, time, duration);
	}
}
