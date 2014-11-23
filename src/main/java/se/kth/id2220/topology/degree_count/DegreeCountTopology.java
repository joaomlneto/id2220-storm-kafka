package se.kth.id2220.topology.degree_count;

import se.kth.id2220.StormTopologyFactory;
import storm.kafka.KafkaSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Performs vertex degree count on a directed graph with data received from
 * Kafka.<br/>
 * Input: a graph edge represeted by two integers separated by a tab
 */
public class DegreeCountTopology implements StormTopologyFactory {

	@Override
	public StormTopology createTopology(KafkaSpout kafkaSpout) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", kafkaSpout, 5);
		builder.setBolt("DegreeCountBolt", new DegreeCountBolt(), 5).shuffleGrouping("KafkaSpout");
		return builder.createTopology();
	}
}
