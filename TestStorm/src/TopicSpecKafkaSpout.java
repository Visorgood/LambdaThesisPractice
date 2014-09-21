import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;


public class TopicSpecKafkaSpout extends KafkaSpout
{
	private static final long serialVersionUID = -4510072461582580828L;
	private SpoutOutputCollector _collector;

	public TopicSpecKafkaSpout(SpoutConfig spoutConf)
	{
		super(spoutConf);
	}

	@Override
	public void nextTuple()
	{
		super.nextTuple();
		Object topic = this.getComponentConfiguration().get("topic");
		Values values = new Values(topic);
		_collector.emit(values);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		//super.declareOutputFields(declarer);
		declarer.declare(new Fields("event", "topic"));
	}
}
