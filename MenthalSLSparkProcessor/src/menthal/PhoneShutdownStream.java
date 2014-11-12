package menthal;

import org.apache.avro.generic.GenericRecord;

public class PhoneShutdownStream  extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 8308012145891965487L;

	public PhoneShutdownStream()
	{
		schemaName = "phone_shutdown";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processPhoneShutdown(userId, time);
	}
}
