import org.apache.avro.generic.GenericRecord;

public class SmsReceivedBolt extends EventProcessingBolt
{
	private static final long serialVersionUID = -4695049338179572315L;
	
	public SmsReceivedBolt()
	{
		schemaName = "sms_received";
	}

	@Override
	protected void processEvent(GenericRecord record)
	{
		// some particular logic must be here!
		System.out.println(record);
		//jedis.hset(schemaName, record.get("id").toString(), record.toString());
	}
}