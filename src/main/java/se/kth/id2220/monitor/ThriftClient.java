package se.kth.id2220.monitor;

import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;

import backtype.storm.generated.Nimbus.Client;

public class ThriftClient {

	public Client getClient(String host, int port) {
		// Set the IP and port of thrift server.
		// By default, the thrift server start on port 6627
		TSocket socket = new TSocket(host, port);
		TFramedTransport tFramedTransport = new TFramedTransport(socket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tFramedTransport);
		Client client = new Client(tBinaryProtocol);
		try {
			// Open the connection with thrift client.
			tFramedTransport.open();
		} catch (Exception exception) {
			throw new RuntimeException("Error occurred while making connection with nimbus thrift server");
		}
		// return the Nimbus Thrift client.
		return client;
	}
}
