package menthal;
import org.apache.avro.generic.GenericRecord;

public class ScreenUnlockBolt extends EventProcessingBolt {
	private static final long serialVersionUID = 3729137447796160786L;

	public ScreenUnlockBolt() {
		schemaName = "screen_unlock";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processScreenUnlock(userId, time);
	}
}
