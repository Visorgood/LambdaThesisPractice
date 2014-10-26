import org.apache.avro.generic.GenericRecord;

public class ScreenOnBolt extends EventProcessingBolt {
	private static final long serialVersionUID = -4822800672865517021L;

	public ScreenOnBolt() {
		schemaName = "screen_on";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		System.out.println(schemaName + "-Bolt: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processScreenOn(userId, time);
	}
}
