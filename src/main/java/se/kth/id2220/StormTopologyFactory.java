package se.kth.id2220;

import org.apache.commons.cli.CommandLine;

import backtype.storm.generated.StormTopology;

public interface StormTopologyFactory {
	public StormTopology createTopology(CommandLine cmd);
}
