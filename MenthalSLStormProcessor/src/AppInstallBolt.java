import org.apache.avro.generic.GenericRecord;

public class AppInstallBolt extends EventProcessingBolt {
	private static final long serialVersionUID = -5069348287754598035L;

	public AppInstallBolt() {
		schemaName = "app_install";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String appName = record.get("appName").toString();
		eventAggregator.processAppInstall(userId, time, appName);
	}
}