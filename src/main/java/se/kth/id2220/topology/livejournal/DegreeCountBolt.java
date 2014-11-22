package se.kth.id2220.topology.livejournal;

import java.util.Hashtable;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class DegreeCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	Hashtable<Integer, Integer> _outDegrees = new Hashtable<Integer, Integer>();
	Hashtable<Integer, Integer> _inDegrees = new Hashtable<Integer, Integer>();
	
	OutputCollector _collector;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String[] edge = input.getString(0).split("\t");
		int v0 = Integer.parseInt(edge[0]);
		int v1 = Integer.parseInt(edge[1]);
		System.out.println(edge[0] + "->" + edge[1]);
		int v0_degree = (_outDegrees.containsKey(v0) ? _outDegrees.get(v0) : 0) + 1;
		int v1_degree = (_inDegrees.containsKey(v1) ? _inDegrees.get(v1) : 0) + 1;
		_outDegrees.put(v0, v0_degree);
		_inDegrees.put(v1, v1_degree);
		/*
		 * System.out.println("************************************************")
		 * ; System.out.println(outDegrees);
		 * System.out.println("************************************************"
		 * ); System.out.println(inDegrees);
		 * System.out.println("************************************************"
		 * );
		 */
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// nothing to declare
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String[] edge = input.getString(0).split("\t");
		int v0 = Integer.parseInt(edge[0]);
		int v1 = Integer.parseInt(edge[1]);
		System.out.println(edge[0] + "->" + edge[1]);
		int v0_degree = (_outDegrees.containsKey(v0) ? _outDegrees.get(v0) : 0) + 1;
		int v1_degree = (_inDegrees.containsKey(v1) ? _inDegrees.get(v1) : 0) + 1;
		_outDegrees.put(v0, v0_degree);
		_inDegrees.put(v1, v1_degree);
		//_collector.ack(input);
	}

}
