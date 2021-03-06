package menthal;
import org.apache.avro.generic.GenericRecord;

public class AppInstallStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -3419094602712244195L;

	public AppInstallStream()
	{
		schemaName = "app_install";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		String appName = record.get("appName").toString();
		long time = (long)record.get("time");
		eventAggregator.processAppInstall(userId, time, appName);
	}
}