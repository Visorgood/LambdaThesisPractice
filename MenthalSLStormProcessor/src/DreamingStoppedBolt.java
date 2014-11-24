import org.apache.avro.generic.GenericRecord;

public class DreamingStoppedBolt extends EventProcessingBolt {
	private static final long serialVersionUID = 5558740613032107100L;

	public DreamingStoppedBolt() {
		schemaName = "dreaming_stopped";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processDreamingStopped(userId, time);
	}
}
