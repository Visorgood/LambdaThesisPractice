import org.apache.avro.generic.GenericRecord;

public class CallMissedBolt extends EventProcessingBolt {
	private static final long serialVersionUID = 397914971678125983L;

	public CallMissedBolt() {
		schemaName = "call_missed";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String contactHash = record.get("contactHash").toString();
		eventAggregator.processCallMissed(userId, time, contactHash, time);
	}
}
