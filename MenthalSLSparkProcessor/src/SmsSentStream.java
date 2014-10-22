import org.apache.avro.generic.GenericRecord;

public class SmsSentStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 2265474416158752690L;

	public SmsSentStream()
	{
		schemaName = "sms_sent";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		String contactHash = (String)record.get("contactHash");
		int msgLength = (int)record.get("msgLength");
		EventAggregator eventAggregator = new EventAggregator("localhost");
		eventAggregator.processSmsSent(userId, time, contactHash, msgLength);
	}
}