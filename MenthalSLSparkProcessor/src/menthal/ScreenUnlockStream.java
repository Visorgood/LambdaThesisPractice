package menthal;
import org.apache.avro.generic.GenericRecord;


public class ScreenUnlockStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 4566891534392181030L;

	public ScreenUnlockStream()
	{
		schemaName = "screen_unlock";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processScreenUnlock(userId, time);
	}
}
