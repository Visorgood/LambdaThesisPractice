import org.apache.avro.generic.GenericRecord;

public class DreamingStartedBolt extends EventProcessingBolt {
	private static final long serialVersionUID = 4217477523494270254L;

	public DreamingStartedBolt() {
		schemaName = "dreaming_started";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processDreamingStarted(userId, time);
	}
}
