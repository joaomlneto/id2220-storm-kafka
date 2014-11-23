package se.kth.id2220;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;

/**
 * A launcher for topologies
 */
public class Topology {

	/**
	 * How many seconds should we run topology in local mode
	 */
	public static final int TOPOLOGY_LOCAL_TIME = 60000; // milliseconds

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException, InstantiationException, IllegalAccessException, ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {

		// parse command line options
		Options options = new Options();
		Option option_cluster = new Option("cluster", true, "where to run the topology? specify 'local' or 'remote'");
		option_cluster.setRequired(true);
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
			formatter.printHelp("id2220-topology", options);
			return;
		}

		// Check which topology to run
		StormTopologyFactory topologyFactory = (StormTopologyFactory) Class.forName(cmd.getOptionValue("topology")).newInstance();
		StormTopology topology = topologyFactory.createTopology(cmd);

		// submit topology
		if (cmd.getOptionValue("cluster").equals("local")) {
			// local cluster mode
			LocalCluster cluster = new LocalCluster();
			Config conf = new Config();
			cluster.submitTopology("DegreeCountTopology", conf, topology);
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
			Config stormConf = new Config();
			if (cmd.hasOption("disableAcks")) {
				stormConf.setNumAckers(0);
			}
			StormSubmitter.submitTopologyWithProgressBar(topologyName, stormConf, topology);
		}
	}
}
