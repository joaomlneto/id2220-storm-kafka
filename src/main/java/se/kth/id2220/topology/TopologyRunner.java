package se.kth.id2220.topology;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

public class TopologyRunner {
	
	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException, InstantiationException, IllegalAccessException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
		// parse command line options
		Options options = new Options();
		
		Option option_cluster = new Option("cluster", true, "where to run the topology? specify 'local' or 'remote'");
		option_cluster.setRequired(true);
		options.addOption("cluster", true, "where to run the topology? specify 'local' or 'remote'");
		options.addOption("topology", true, "which topology to run?");
		options.addOption("name", true, "[remote-only] name of the topology");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args, false);
		
		// TODO: validate input arguments
		
		// Check which topology to run
		StormTopologyFactory topologyFactory = (StormTopologyFactory) Class.forName(cmd.getOptionValue("topology")).newInstance();
		StormTopology topology = topologyFactory.createTopology();
		
		// submit topology
		if(cmd.getOptionValue("cluster") == null || cmd.getOptionValue("cluster").equals("local")) {
			// local cluster mode
			LocalCluster cluster = new LocalCluster();
			Config conf = new Config();
			cluster.submitTopology("DegreeCountTopology", conf, topology);
			try {
				// Wait for some time before exiting
				Thread.sleep(20000);
			} catch (Exception exception) {
				System.out.println("Thread interrupted exception : " + exception);
			}
			cluster.killTopology("DegreeCountTopology");
			cluster.shutdown();
		}
		else {
			// remote cluster mode
			String topologyName = cmd.getOptionValue("name");
			Config stormConf = new Config();
			StormSubmitter.submitTopologyWithProgressBar(topologyName, stormConf, topology);
		}
	}
}
