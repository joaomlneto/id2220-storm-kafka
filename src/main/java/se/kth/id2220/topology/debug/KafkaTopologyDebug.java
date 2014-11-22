package se.kth.id2220.topology.debug;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;


public class KafkaTopologyDebug {
	
	private static String topic = "livejournal";
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		// configure kafka spout
		ZkHosts zkHosts = new ZkHosts("localhost:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", "id7");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.forceFromStart = true;
		
		// create topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
		builder.setBolt("PrinterBolt", new PrinterBolt(), 1).globalGrouping("KafkaSpout");
		
		// setup local cluster and run for 10 seconds
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		cluster.submitTopology("KafkaToplogy", conf, builder.createTopology());
		try {
			// Wait for some time before exiting
			System.out.println("Waiting to consume from kafka. Topic: " + topic);
			Thread.sleep(30000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		cluster.killTopology("KafkaToplogy");
		cluster.shutdown();
	}
}
