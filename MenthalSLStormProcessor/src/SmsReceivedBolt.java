import org.apache.avro.generic.GenericRecord;

public class SmsReceivedBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = -4695049338179572315L;
	
	public SmsReceivedBolt()
	{
		schemaName = "sms_received";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		String contactHash = (String)record.get("contactHash");
		int msgLength = (int)record.get("msgLength");
		eventAggregator.processSmsReceived(userId, time, contactHash, msgLength);
	}
}