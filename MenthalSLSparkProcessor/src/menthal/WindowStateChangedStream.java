package menthal;

import org.apache.avro.generic.GenericRecord;

public class WindowStateChangedStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -5920262639292760259L;

	public WindowStateChangedStream()
	{
		schemaName = "window_state_changed";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String appName = record.get("appName").toString();
		String windowTitle = record.get("windowTitle").toString();
		eventAggregator.processWindowStateChanged(userId, time, appName, windowTitle);
	}
}
