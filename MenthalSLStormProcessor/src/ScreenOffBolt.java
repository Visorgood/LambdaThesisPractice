import org.apache.avro.generic.GenericRecord;

public class ScreenOffBolt extends EventProcessingBolt {
	private static final long serialVersionUID = -5619442309771206397L;

	public ScreenOffBolt() {
		schemaName = "screen_off";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		System.out.println(schemaName + "-Bolt: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processScreenOff(userId, time);
	}
}
