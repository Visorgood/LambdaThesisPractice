package menthal;
import org.apache.avro.generic.GenericRecord;


public class AppSessionStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -7349114140119314283L;

	public AppSessionStream()
	{
		schemaName = "app_session";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		long duration = (long)record.get("duration");
		String appName = record.get("appName").toString();
		eventAggregator.processAppSession(userId, time, duration, appName);
	}
}