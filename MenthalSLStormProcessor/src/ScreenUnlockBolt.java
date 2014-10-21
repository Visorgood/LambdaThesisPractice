import org.apache.avro.generic.GenericRecord;

public class ScreenUnlockBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = 3729137447796160786L;

	public ScreenUnlockBolt()
	{
		schemaName = "screen_unlock";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		long userId = (long)record.get("user_id");
		long time = (long)record.get("time");
		eventAggregator.processScreenUnlock(userId, time);
	}
}
