import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class MainTopology {	
	private static String[] eventNames = new String[] {
		"app_install", "app_session" , "call_missed", "call_outgoing",
		"call_received", "dreaming_started", "dreaming_stopped", "phone_shutdown",
		"screen_off", "screen_on", "screen_unlock", "sms_received",
		"sms_sent", "window_state_changed"};
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		for (String eventName : eventNames) {
			ZkHosts zkHosts = new ZkHosts("localhost:5181","/brokers");
			String topic = eventName;
			// Root path in Zookeeper for the spout to store consumer offsets
			String zkRoot = "/kafka-spout";
			// ID for storing consumer offsets in Zookeeper
			// The spout appends this id to zkRoot when composing its ZooKeeper path.  You don't need a leading `/`.
			String zkSpoutId = eventName;
			SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
			spoutConfig.zkPort = 5181;
			spoutConfig.startOffsetTime = -2;
			spoutConfig.forceFromStart = true;
			// for each event type we create one kafka spout and one bolt of specific subclass
			// for each event type kafka must have dedicated topic
			topologyBuilder.setSpout(eventName + "-KafkaSpout", new KafkaSpout(spoutConfig), 1);
			topologyBuilder.setBolt(eventName + "-Bolt", EventProcessingBolt.getEventProcessingBoltByEventName(eventName), 1).shuffleGrouping(eventName + "-KafkaSpout");
		}
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(1);
			StormTopology topology = topologyBuilder.createTopology();
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology);
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test-topology", conf, topologyBuilder.createTopology());
			Utils.sleep(20000);
			cluster.killTopology("test-topology");
			cluster.shutdown();
    	}
	}
}
