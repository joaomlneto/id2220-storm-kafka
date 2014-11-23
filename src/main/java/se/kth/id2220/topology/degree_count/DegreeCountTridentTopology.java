package se.kth.id2220.topology.degree_count;

import org.apache.commons.cli.CommandLine;

import se.kth.id2220.StormTopologyFactory;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * Performs vertex degree count on a directed graph with data received from
 * Kafka.<br/>
 * Input: a graph edge represeted by two integers separated by a tab
 */
public class DegreeCountTridentTopology implements StormTopologyFactory {

	@Override
	public StormTopology createTopology(CommandLine cmd) {
		// create kafka spout
		ZkHosts zkHosts = new ZkHosts(cmd.getOptionValue("kafka"));
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zkHosts, "testing");
		TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
		
		// create topology
		TridentTopology topology = new TridentTopology();
		topology.newStream("kafkaSpout", kafkaSpout)
			.shuffle()
			.each(new Fields("bytes"), new DegreeCountFunction(), new Fields());
		return topology.build();
	}
}
