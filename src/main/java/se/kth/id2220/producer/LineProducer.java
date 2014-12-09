package se.kth.id2220.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import se.kth.id2220.KafkaProducer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Kafka producer where each messages submitted is a line from the file. <br/>
 * Lines starting with '#' are ignored
 */
public class LineProducer implements KafkaProducer {

	public static final int FEEDBACK_INTERVAL = 10000;
	
	static final MetricRegistry producerMetrics = new MetricRegistry();

	@Override
	public void produce(ProducerConfig kafkaConfig, BufferedReader reader, String topic) throws IOException {
		String line;

		// Setup Kafka producer
		Producer<String, String> producer = new Producer<String, String>(kafkaConfig);
		
		// Setup metrics
		ConsoleReporter reporter = ConsoleReporter.forRegistry(producerMetrics).convertRatesTo(TimeUnit.SECONDS).build();
		reporter.start(5, TimeUnit.SECONDS);
		Meter records = producerMetrics.meter("records");

		// Produce loop
		while ((line = reader.readLine()) != null) {
			// do not process comments
			if (line.startsWith("#"))
				continue;

			// send data to kafka
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, line);
			producer.send(data);

			// give some feedback
			records.mark();
		}
	}
}
