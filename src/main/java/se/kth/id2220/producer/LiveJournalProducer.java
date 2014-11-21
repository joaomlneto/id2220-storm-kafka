package se.kth.id2220.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class LiveJournalProducer {
	public static final String TOPIC_ID = "livejournal";
	public static final String KAFKA_HOST = "localhost:9092";
	
	private static String fileURL = "/Users/anatoly/Desktop/livejournal.txt";

	public static void main(String[] args) throws IOException {
		
		// process cmd arguments
		if(args.length > 0) {
			fileURL = args[0];
		}
		
		// open dataset file
		BufferedReader reader = new BufferedReader(new FileReader(fileURL));

		// open connection to kafka
		Properties props = new Properties();
		props.put("metadata.broker.list", KAFKA_HOST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		// send dataset
		String line;
		int tick = 0; // XXX DEBUG
		while ((line = reader.readLine()) != null) {
			// do not process comments
			if (line.startsWith("#"))
				continue;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC_ID, line);
			producer.send(data);
			// XXX DEBUG just to give some feedback
			if(tick == 50) break; // FIXME DEBUG
			if ((tick++ % 68994) == 0) {
				Double percent = (((double) tick) / 68993777 * 100);
				System.out.println("" + percent + "% completed");
			}
		}
		producer.close();
	}
}
