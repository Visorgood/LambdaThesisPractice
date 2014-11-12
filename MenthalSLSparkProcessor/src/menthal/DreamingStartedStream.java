package menthal;

import org.apache.avro.generic.GenericRecord;

public class DreamingStartedStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = -8513942781086351154L;

	public DreamingStartedStream()
	{
		schemaName = "dreaming_started";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processDreamingStarted(userId, time);
	}
}
