package se.kth.id2220.topology.degree_count;

import org.apache.commons.cli.CommandLine;

import se.kth.id2220.StormTopologyFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

/**
 * Performs vertex degree count on a directed graph with data received from
 * Kafka.<br/>
 * Input: a graph edge represeted by two integers separated by a tab
 */
public class DegreeCountTopology implements StormTopologyFactory {

	@Override
	public StormTopology createTopology(CommandLine cmd) {
		// setup kafka spout
		ZkHosts zkHosts = new ZkHosts(cmd.getOptionValue("kafka"));
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, cmd.getOptionValue("topic"), "", "id7");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
		
		// build topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", kafkaSpout, 5);
		builder.setBolt("DegreeCountBolt", new DegreeCountBolt(), 5).shuffleGrouping("KafkaSpout");
		return builder.createTopology();
	}
}
