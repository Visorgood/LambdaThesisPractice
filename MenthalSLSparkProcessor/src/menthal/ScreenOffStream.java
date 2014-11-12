package menthal;
import org.apache.avro.generic.GenericRecord;


public class ScreenOffStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 921726226747459948L;

	public ScreenOffStream()
	{
		schemaName = "screen_off";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processScreenOff(userId, time);
	}
}
