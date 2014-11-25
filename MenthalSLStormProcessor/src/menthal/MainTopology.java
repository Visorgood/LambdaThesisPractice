package menthal;
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
  private static String[] eventNames = new String[] {"app_install", "app_session", "call_missed",
      "call_outgoing", "call_received", "dreaming_started", "dreaming_stopped", "phone_shutdown",
      "screen_off", "screen_on", "screen_unlock", "sms_received", "sms_sent",
      "window_state_changed"};

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    for (String eventName : eventNames) {
      ZkHosts zkHosts = new ZkHosts("localhost:5181", "/brokers");
      String topic = eventName;
      // Root path in Zookeeper for the spout to store consumer offsets
      String zkRoot = "/kafka-spout";
      // ID for storing consumer offsets in Zookeeper
      // The spout appends this id to zkRoot when composing its ZooKeeper path. You don't need a
      // leading `/`.
      String zkSpoutId = eventName;
      SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
      spoutConfig.zkPort = 5181;
      spoutConfig.startOffsetTime = -2;
      spoutConfig.forceFromStart = true;
      // for each event type we create one kafka spout and one bolt of specific subclass
      // for each event type kafka must have dedicated topic
      KafkaSpout spout = new KafkaSpout(spoutConfig);
      String spoutName = eventName + "-KafkaSpout";
      topologyBuilder.setSpout(spoutName, spout, 1);
      System.out.println("Spout set: " + spoutName + " " + spout.getClass().toString());
      EventProcessingBolt bolt = EventProcessingBolt.getEventProcessingBoltByEventName(eventName);
      String boltName = eventName + "-Bolt";
      topologyBuilder.setBolt(boltName, bolt, 1).shuffleGrouping(spoutName);
      System.out.println("Bolt set: " + boltName + " " + bolt.getClass().toString());
    }

    // for anomaly detection
    // AnomalyDetectionBolt anomalyDetectionBolt = new AnomalyDetectionBolt();
    // topologyBuilder.setBolt("AnomalyDetectionBolt", anomalyDetectionBolt,
    // 1).shuffleGrouping("sms_sent-Bolt");
    // topologyBuilder.setBolt("AnomalyDetectionBolt1", anomalyDetectionBolt,
    // 1).shuffleGrouping("call_outgoing-Bolt");
    // topologyBuilder.setBolt("AnomalyDetectionBolt2", anomalyDetectionBolt,
    // 1).shuffleGrouping("app_install-Bolt");
    // topologyBuilder.setBolt("AnomalyDetectionBolt3", anomalyDetectionBolt,
    // 1).shuffleGrouping("phone_shutdown-Bolt");

    Config conf = new Config();
    
    if (args != null && args.length > 0) {
      System.out.println("Topology will be submitted in the cluster mode.");
      conf.setNumWorkers(Integer.parseInt(args[0]));
      StormTopology topology = topologyBuilder.createTopology();
      StormSubmitter.submitTopology("real-topology", conf, topology);
    } else {
      System.out.println("Topology will be submitted in the local mode.");
      conf.setDebug(true);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test-topology", conf, topologyBuilder.createTopology());
      Utils.sleep(20000);
      cluster.killTopology("test-topology");
      cluster.shutdown();
    }
  }
}
