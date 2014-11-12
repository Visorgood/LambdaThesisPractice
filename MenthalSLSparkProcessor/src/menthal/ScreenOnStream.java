package menthal;

import org.apache.avro.generic.GenericRecord;

public class ScreenOnStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -7685231796752491144L;

	public ScreenOnStream()
	{
		schemaName = "screen_on";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processScreenOn(userId, time);
	}
}
