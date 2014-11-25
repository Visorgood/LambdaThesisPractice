package menthal;
import org.apache.avro.generic.GenericRecord;

public class WindowStateChangedBolt extends EventProcessingBolt {
	private static final long serialVersionUID = -3531767065474058666L;

	public WindowStateChangedBolt() {
		schemaName = "window_state_changed";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String appName = record.get("appName").toString();
		String windowTitle = record.get("windowTitle").toString();
		eventAggregator.processWindowStateChanged(userId, time, appName, windowTitle);
	}
}
