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
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}
