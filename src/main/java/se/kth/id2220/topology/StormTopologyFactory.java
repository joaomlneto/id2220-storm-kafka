package se.kth.id2220.topology;

import backtype.storm.generated.StormTopology;

public interface StormTopologyFactory {
	public StormTopology createTopology();
}
