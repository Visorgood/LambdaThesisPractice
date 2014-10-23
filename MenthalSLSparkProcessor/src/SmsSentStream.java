import org.apache.avro.generic.GenericRecord;

public class SmsSentStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 2265474416158752690L;

	public SmsSentStream()
	{
		schemaName = "sms_sent";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String contactHash = record.get("contactHash").toString();
		int msgLength = (int)record.get("msgLength");
		eventAggregator.processSmsSent(userId, time, contactHash, msgLength);
	}
}