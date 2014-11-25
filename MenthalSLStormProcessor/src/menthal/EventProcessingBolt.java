package menthal;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.joda.time.DateTime;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

// base class for all bolts, that process events of different types
// has a field schemaName, that must be initialized in the constructor of a specific bolt
// has an abstract method processEvent, that must be overwritten in each specific bolt class
// has a method getEventProcessingBoltByEventName, that returns a new object of a particular bolt, specified by event name

public abstract class EventProcessingBolt extends BaseRichBolt {	
	private static final long serialVersionUID = -8386633614335300892L;
	
	protected String schemaName;
	protected EventAggregator eventAggregator;
	OutputCollector _collector;
	
	public static EventProcessingBolt getEventProcessingBoltByEventName(String eventName) {
		switch (eventName) {
			case "app_install": return new AppInstallBolt();
			case "app_session": return new AppSessionBolt();
			case "call_missed": return new CallMissedBolt();
			case "call_outgoing": return new CallOutgoingBolt();
			case "call_received": return new CallReceivedBolt();
			case "dreaming_started": return new DreamingStartedBolt();
			case "dreaming_stopped": return new DreamingStoppedBolt();
			case "phone_shutdown": return new PhoneShutdownBolt();
			case "screen_off": return new ScreenOffBolt();
			case "screen_on": return new ScreenOnBolt();
			case "screen_unlock": return new ScreenUnlockBolt();
			case "sms_received": return new SmsReceivedBolt();
			case "sms_sent": return new SmsSentBolt();
			case "window_state_changed": return new WindowStateChangedBolt();
		}
		return null;
	}

	protected abstract void processEvent(GenericRecord record);
	
	@Override
	public void execute(Tuple tuple) {
		try	{
			Schema schema = new Schema.Parser().parse(new File(schemaName + ".avsc"));
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			InputStream in = new ByteArrayInputStream((byte[])tuple.getValue(0));
			GenericRecord record = datumReader.read(null, DecoderFactory.get().jsonDecoder(schema, in));
			System.out.printf("%s:%s:%s%n", DateTime.now().toString(), this.getClass().toString(), record.toString());
			processEvent(record);
			_collector.emit(new Values(record));
		} catch (Exception e) {
			System.out.println("Exception raised!");
			System.out.println(e.getMessage());
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		eventAggregator = new RedisEventAggregator("localhost");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record"));
	}
}
