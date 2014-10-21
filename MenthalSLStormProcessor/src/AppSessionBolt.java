import org.apache.avro.generic.GenericRecord;

public class AppSessionBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = 2369477756990536798L;

	public AppSessionBolt()
	{
		schemaName = "app_session";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}
