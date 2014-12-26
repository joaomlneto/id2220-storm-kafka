package se.kth.id2220;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

/**
 * A launcher for topologies
 */
public class Topology {

	/**
	 * How many seconds should we run topology in local mode
	 */
	public static final int TOPOLOGY_LOCAL_TIME = 60*1000; // milliseconds

	public static void main(String[] args) throws Exception /* gotta catch 'em all! */ {

		// parse command line options
		Options options = new Options();
		options.addOption("cluster", true, "where to run the topology? specify 'local' or 'remote'");
		options.addOption("topology", true, "which topology to run?");
		options.addOption("name", true, "[remote-only] name of the topology");
		options.addOption("nack", false, "check wether to disable tuple acknowledgement");
		options.addOption("kafka", true, "kafka host");
		options.addOption("topic", true, "kafka topic to consume from");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		// validate input arguments
		boolean input_errors = false;
		if (!cmd.hasOption("cluster") || !cmd.hasOption("topology") || !cmd.hasOption("name") || !cmd.hasOption("kafka") || !cmd.hasOption("topic")) {
			System.out.println("arguments missing");
			input_errors = true;
		}
		if (!cmd.getOptionValue("cluster").equals("local") && !cmd.getOptionValue("cluster").equals("remote")) {
			System.out.println("cluster must be 'local' or 'remote'");
			input_errors = true;
		}
		if (input_errors) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("se.kth.id2220.Topology", options);
			return;
		}
		
		// print what's going to happen
		System.out.println("[ID2220] Launching topology");
		System.out.println("Cluster Mode:   " + cmd.getOptionValue("cluster"));
		System.out.println("Topology Class: " + cmd.getOptionValue("topology"));
		System.out.println("Topology Name:  " + cmd.getOptionValue("name"));
		System.out.println("Kafka host:     " + cmd.getOptionValue("kafka"));
		System.out.println("Kafka topic:    " + cmd.getOptionValue("topic"));
		System.out.println("Disable Acks:   " + (cmd.hasOption("nack") ? "yes" : "no"));

		// Check which topology to run
		StormTopologyFactory topologyFactory = (StormTopologyFactory) Class.forName(cmd.getOptionValue("topology")).newInstance();
		StormTopology topology = topologyFactory.createTopology(cmd);
		
		// Storm configuration
		Config stormConf = new Config();
		
		// Storm cluster default configuration values
		stormConf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 128);

		// submit topology
		if (cmd.getOptionValue("cluster").equals("local")) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("DegreeCountTopology", stormConf, topology);
			try {
				// Wait for some time before exiting
				Thread.sleep(TOPOLOGY_LOCAL_TIME);
			} catch (Exception exception) {
				System.out.println("Thread interrupted exception : " + exception);
			}
			cluster.killTopology("DegreeCountTopology");
			cluster.shutdown();
		} else {
			// remote cluster mode
			String topologyName = cmd.getOptionValue("name");
			if (cmd.hasOption("disableAcks")) {
				stormConf.setNumAckers(0);
			}
			StormSubmitter.submitTopologyWithProgressBar(topologyName, stormConf, topology);
		}
	}
}
