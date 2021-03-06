package menthal;
import org.apache.avro.generic.GenericRecord;

public class SmsReceivedStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -7032031544087846932L;

	public SmsReceivedStream()
	{
		schemaName = "sms_received";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String contactHash = record.get("contactHash").toString();
		int msgLength = (int)record.get("msgLength");
		eventAggregator.processSmsReceived(userId, time, contactHash, msgLength);
	}
}