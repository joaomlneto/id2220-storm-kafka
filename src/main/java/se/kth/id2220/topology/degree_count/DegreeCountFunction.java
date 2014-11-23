package se.kth.id2220.topology.degree_count;

import java.util.Hashtable;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DegreeCountFunction extends BaseFunction {

	private static final long serialVersionUID = 1L;

	Hashtable<Integer, Integer> outDegrees = new Hashtable<Integer, Integer>();
	Hashtable<Integer, Integer> inDegrees = new Hashtable<Integer, Integer>();

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String[] edge = tuple.getStringByField("str").split("\t");
		int v0 = Integer.parseInt(edge[0]);
		int v1 = Integer.parseInt(edge[1]);
		// System.out.println(edge[0] + "->" + edge[1]);
		int v0_degree = (outDegrees.containsKey(v0) ? outDegrees.get(v0) : 0) + 1;
		int v1_degree = (inDegrees.containsKey(v1) ? inDegrees.get(v1) : 0) + 1;
		outDegrees.put(v0, v0_degree);
		inDegrees.put(v1, v1_degree);
		/*
		 * System.out.println("************************************************")
		 * ; System.out.println(outDegrees);
		 * System.out.println("************************************************"
		 * ); System.out.println(inDegrees);
		 * System.out.println("************************************************"
		 * );
		 */
	}

}
