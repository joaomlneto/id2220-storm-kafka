package se.kth.id2220.topology.livejournal.simple;

import se.kth.id2220.topology.StormTopologyFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class DegreeCountTopology implements StormTopologyFactory {

	private static String topic = "livejournal";

	@Override
	public StormTopology createTopology() {
		
		// configure kafka spout
		ZkHosts zkHosts = new ZkHosts("10.20.0.39:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", "id7");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.forceFromStart = true;

		// create topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 5);
		builder.setBolt("DegreeCountBolt", new DegreeCountBolt(), 5).shuffleGrouping("KafkaSpout");
		
		return builder.createTopology();
	}
}
