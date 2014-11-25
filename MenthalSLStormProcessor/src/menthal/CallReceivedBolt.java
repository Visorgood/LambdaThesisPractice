package menthal;
import org.apache.avro.generic.GenericRecord;

public class CallReceivedBolt extends EventProcessingBolt {
	private static final long serialVersionUID = -7580395188906387752L;

	public CallReceivedBolt() {
		schemaName = "call_received";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		String contactHash = record.get("contactHash").toString();
		long duration = (long)record.get("durationInMillis");
		eventAggregator.processCallReceived(userId, time, contactHash, time, duration);
	}
}
