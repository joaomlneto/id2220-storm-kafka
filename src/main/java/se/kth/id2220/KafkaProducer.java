package se.kth.id2220;

import java.io.BufferedReader;
import java.io.IOException;

import kafka.producer.ProducerConfig;

public interface KafkaProducer {
	public void produce(ProducerConfig kafkaConfig, BufferedReader reader, String topic) throws IOException;
}
