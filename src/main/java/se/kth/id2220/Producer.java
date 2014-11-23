package se.kth.id2220;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.producer.ProducerConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Starts all kinds of Kafka producers that read input from a file
 */
public class Producer {
	public static void main(String[] args) throws ParseException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {

		// parse command line options
		Options options = new Options();
		Option option_cluster = new Option("cluster", true, "where to run the topology? specify 'local' or 'remote'");
		option_cluster.setRequired(true);
		options.addOption("producer", true, "which producer to run?");
		options.addOption("kafka", true, "kafka metadata broker host (zookeeper)");
		options.addOption("topic", true, "kafka topic");
		options.addOption("file", true, "file with the data to be read");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		// validate input arguments
		if (!cmd.hasOption("producer") || !cmd.hasOption("kafka") || !cmd.hasOption("topic") || !cmd.hasOption("file")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("id2220-producer", options);
			return;
		}

		System.out.println(cmd.getOptionValue("producer"));
		System.out.println(cmd.getOptionValue("kafka"));
		System.out.println(cmd.getOptionValue("topic"));
		System.out.println(cmd.getOptionValue("file"));

		// setup configuration for Kafka
		Properties kafkaProps = new Properties();
		kafkaProps.put("metadata.broker.list", cmd.getOptionValue("kafka"));
		kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
		kafkaProps.put("request.required.acks", "1");
		ProducerConfig kafkaConfig = new ProducerConfig(kafkaProps);
		
		// open dataset file
		BufferedReader reader = new BufferedReader(new FileReader(cmd.getOptionValue("file")));

		// Check which topology to run
		KafkaProducer producer__xx = (KafkaProducer) Class.forName(cmd.getOptionValue("producer")).newInstance();

		// finally, produce!
		producer__xx.produce(kafkaConfig, reader, cmd.getOptionValue("topic"));

		// clean up
		reader.close();
	}
}
