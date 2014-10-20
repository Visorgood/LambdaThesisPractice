import org.apache.avro.generic.GenericRecord;

public class AppInstallBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = -5069348287754598035L;

	public AppInstallBolt()
	{
		schemaName = "app_install";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		System.out.println(schemaName + "-Bolt: " + record.toString());
		jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}