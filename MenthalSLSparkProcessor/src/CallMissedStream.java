import org.apache.avro.generic.GenericRecord;


public class CallMissedStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 5172374523308906L;

	public CallMissedStream()
	{
		schemaName = "call_missed";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		String contactHash = (String)record.get("contactHash");
		eventAggregator.processCallMissed(userId, time, contactHash, time);
	}
}
