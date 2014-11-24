import org.apache.avro.generic.GenericRecord;

public class AppSessionBolt extends EventProcessingBolt {
	private static final long serialVersionUID = 2369477756990536798L;

	public AppSessionBolt() {
		schemaName = "app_session";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		long duration = (long)record.get("duration");
		String appName = record.get("appName").toString();
		eventAggregator.processAppSession(userId, time, duration, appName);
	}
}
