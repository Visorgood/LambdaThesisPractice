import org.apache.avro.generic.GenericRecord;

public class AppInstallStream extends EventProcessingStream implements java.io.Serializable
{
	public AppInstallStream()
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