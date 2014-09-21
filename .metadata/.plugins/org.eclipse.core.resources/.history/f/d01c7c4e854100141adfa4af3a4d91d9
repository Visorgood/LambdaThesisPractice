import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TestTopology
{
	public static void main(String[] args) throws Exception
	{
		TopologyBuilder builder = new TopologyBuilder();
		   
		ZkHosts zkHosts = new ZkHosts("localhost:5181","/brokers");
		String topic = "test";
		// Root path in Zookeeper for the spout to store consumer offsets
		String zkRoot = "/kafka-spout";
		// ID for storing consumer offsets in Zookeeper
		// The spout appends this id to zkRoot when composing its ZooKeeper path.  You don't need a leading `/`.
		String zkSpoutId = "kafka-storm-starter";
		
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
		spoutConfig.zkPort = 5181;
		spoutConfig.startOffsetTime = -2;
		spoutConfig.forceFromStart = true;
		
		KafkaSpout kafkaSpout = new TopicSpecKafkaSpout(spoutConfig);
		
		builder.setSpout("kafka-spout", kafkaSpout, 1);
		builder.setBolt("aggregation", new EventProcessingBolt(), 1).shuffleGrouping("kafka-spout");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if (args != null && args.length > 0)
		{
			conf.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		}
		else
		{
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(30000);
			cluster.killTopology("test");
			cluster.shutdown();
    	}
	}
}