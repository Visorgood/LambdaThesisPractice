package menthal;

import org.apache.avro.generic.GenericRecord;

public class DreamingStoppedStream extends EventProcessingStream implements java.io.Serializable
{
	private static final long serialVersionUID = 624989176585380400L;

	public DreamingStoppedStream()
	{
		schemaName = "dreaming_stopped";
	}

	@Override
	protected void processEvent(GenericRecord record, EventAggregator eventAggregator)
	{
		System.out.println(schemaName + "-Stream: " + record.toString());
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		eventAggregator.processDreamingStopped(userId, time);
	}

}
