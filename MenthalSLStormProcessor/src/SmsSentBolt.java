import org.apache.avro.generic.GenericRecord;

public class SmsSentBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = -7020024151853656410L;

	public SmsSentBolt()
	{
		schemaName = "sms_sent";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		String contactHash = (String)record.get("contactHash");
		int msgLength = (int)record.get("msgLength");
		eventAggregator.processSmsSent(userId, time, contactHash, msgLength);
	}
}