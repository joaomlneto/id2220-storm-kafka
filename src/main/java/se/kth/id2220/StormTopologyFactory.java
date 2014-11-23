package se.kth.id2220;

import storm.kafka.KafkaSpout;
import backtype.storm.generated.StormTopology;

public interface StormTopologyFactory {
	public StormTopology createTopology(KafkaSpout kafkaSpout);
}
