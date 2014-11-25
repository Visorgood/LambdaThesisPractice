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

  // args[0] : int - 0 is local mode, 1 is cluster mode
  // args[1] : int - the time of topology life in the local mode, or the number of workers in the cluster mode
  
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
    // topologyBuilder.setBolt("AnomalyDetectionBolt", anomalyDetectionBolt, 1).shuffleGrouping("sms_sent-Bolt");
    // topologyBuilder.setBolt("AnomalyDetectionBolt1", anomalyDetectionBolt, 1).shuffleGrouping("call_outgoing-Bolt");
    // topologyBuilder.setBolt("AnomalyDetectionBolt2", anomalyDetectionBolt, 1).shuffleGrouping("app_install-Bolt");
    // topologyBuilder.setBolt("AnomalyDetectionBolt3", anomalyDetectionBolt, 1).shuffleGrouping("phone_shutdown-Bolt");

    boolean localMode = (args == null || args.length == 0);
    if (!localMode) {
      int arg0 = Integer.parseInt(args[0]);
      localMode = (arg0 == 0);
    }
    
    Config conf = new Config();
    conf.put("debug", localMode);
    conf.put("eventCountLimit", (args.length > 2 ? Integer.parseInt(args[2]) : 1000));
    
    if (localMode) {
      System.out.println("Topology will be submitted in the local mode.");
      int localModeTime = 20000;
      if (args.length > 1) {
        localModeTime = Math.max(Integer.parseInt(args[1]), 1000);
      }
      conf.setDebug(true);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test-topology", conf, topologyBuilder.createTopology());
      Utils.sleep(localModeTime);
      cluster.killTopology("test-topology");
      cluster.shutdown();
    } else {
      System.out.println("Topology will be submitted in the cluster mode.");
      int numWorkers = 1;
      if (args.length > 1) {
        numWorkers = Math.max(Integer.parseInt(args[1]), 1);
      }
      conf.setNumWorkers(numWorkers);
      StormTopology topology = topologyBuilder.createTopology();
      StormSubmitter.submitTopology("real-topology", conf, topology);
    }
  }
}
