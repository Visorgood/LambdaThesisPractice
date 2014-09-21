import redis.clients.jedis.Jedis;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public final class TestBolt extends BaseBasicBolt
{
	private static final long serialVersionUID = 1560124722884291352L;

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector)
	{
		try
		{
			SmsReceived smsReceived = (SmsReceived)tuple.getValue(0);
			
			System.out.print(smsReceived.toString());
			// do some cool logic!
			
			Jedis jedis = new Jedis("localhost");
			jedis.set("somekey1", "somevalue1");
			String value = jedis.get("somekey1");
			
			//collector.emit(new Values(word, count));
		}
		catch (Exception e)
		{
			System.out.print("Exception raised!");
			System.out.print(e.getMessage());
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
    	declarer.declare(new Fields("word", "count"));
    }
}