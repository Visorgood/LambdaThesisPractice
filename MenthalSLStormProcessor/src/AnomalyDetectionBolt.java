import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class AnomalyDetectionBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2017217675603119343L;
	private static Jedis jedis;
	private static AnomalyDetection anomalyDetection; 
	private static int ANOMALY_CHECK_INTERVAL = 1800000; // 30min
	private static int TTL = 3600000; // 1hour 
	
	private static String[] eventNames = new String[] {"app_install", "call_outgoing", "phone_shutdown", "sms_sent"};
	
	@Override
	 public void prepare(Map conf, TopologyContext context, OutputCollector collector)
	 {
		jedis = new Jedis("localhost");
		anomalyDetection = new AnomalyDetection();
	 }

	@Override
    public void execute(Tuple tuple)
	{
		GenericRecord record = (GenericRecord)tuple.getValue(0);
		String eventName = tuple.getSourceComponent().replace("-Bolt", "");
		long userId = (long)record.get("userId");
		long time = (long)record.get("time");
		
		String eventKey = String.format("user:user%d:%s", userId, eventName);
		jedis.zadd(eventKey, time, String.valueOf(time));
		
		String timeKey = String.format("user:user%d:%s", userId, "lastCheckTime");
		String lastCheckTime = jedis.get(timeKey);
		
		if (lastCheckTime == null)
		{
			jedis.set(timeKey, String.valueOf(time));
			return;
		}
		
		if (time - Long.parseLong(lastCheckTime) > ANOMALY_CHECK_INTERVAL)
		{
			double[] features = new double[eventNames.length];
			
			for (int i = 0; i < eventNames.length; i++)
			{
				eventKey = String.format("user:user%d:%s", userId, eventNames[i]);
				jedis.zremrangeByScore(eventKey, 0, time - TTL);
				features[i] = jedis.zcard(eventKey);
			}
			
			try
			{
				anomalyDetection.run(features);
			}
			catch(Exception e)
			{
				System.out.println("Exception raised!");
				System.out.println(e.getMessage());
			}
			jedis.set(timeKey, String.valueOf(time));
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    } 

}
