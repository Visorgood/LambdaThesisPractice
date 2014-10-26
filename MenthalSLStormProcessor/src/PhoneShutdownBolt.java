import org.apache.avro.generic.GenericRecord;

public class PhoneShutdownBolt extends EventProcessingBolt {
	private static final long serialVersionUID = -6496631773419252572L;

	public PhoneShutdownBolt() {
		schemaName = "phone_shutdown";
	}

	@Override
	protected void processEvent(GenericRecord record) {
		System.out.println(schemaName + "-Bolt: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processPhoneShutdown(userId, time);
	}
}
