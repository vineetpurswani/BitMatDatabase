package org.bitmat.extras;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;


public class TripleIndexing {
	public static void printMap(Map mp) {
		Iterator it = mp.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			System.out.println(pair.getKey() + " = " + pair.getValue());
			it.remove(); // avoids a ConcurrentModificationException
		}
	}

	public static void main (String[] args) throws IOException, ClassNotFoundException {
		final String filename = "/home/fachika/Downloads/sp2b/bin/sp2b.n3";

		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Runnable parser = new Runnable() {
			@Override
			public void run() {
				RDFDataMgr.parse(inputStream, filename, Lang.N3);
			}
		};

		executor.submit(parser);

		HashMap<String, Long> somap = new HashMap<String, Long>();
		Long socounter=0L, pcounter=0L;
		PrintStream out = new PrintStream(new FileOutputStream("/home/fachika/Downloads/sp2b/bin/sp2b.bt"), true);
		PrintStream mapout = new PrintStream(new FileOutputStream("/home/fachika/Downloads/sp2b/bin/sp2b.map"), true);

		while (iter.hasNext()) {
			Triple next = iter.next();
			System.out.println(next.getSubject().toString() + " " + next.getPredicate().toString() + " " + next.getObject().toString());
			if (!somap.containsKey(next.getSubject().toString())) {
				somap.put(next.getSubject().toString(), socounter++);
				mapout.println(next.getSubject().toString() + " " + (socounter-1));
			}
			if (!somap.containsKey(next.getObject().toString())) {
				String key;
				if (next.getObject().isLiteral()) key = "\"" + next.getObject().getLiteralValue() + "\"^^" + next.getObject().getLiteralDatatypeURI().toString();
				else key = next.getObject().toString();
				somap.put(key, socounter++);
				mapout.println(key +  " " + (socounter-1));
			}
			if (!somap.containsKey(next.getPredicate().toString())) {
				somap.put(next.getPredicate().toString(), pcounter++);
				mapout.println(next.getPredicate().toString() +  " " + (pcounter-1));
			}
			out.println(somap.get(next.getSubject().toString()) + " " + somap.get(next.getPredicate().toString()) + " " + somap.get(next.getObject().toString()));

		}
		out.close();
		mapout.close();

		ObjectOutputStream objout = new ObjectOutputStream(new FileOutputStream("/home/fachika/Downloads/sp2b/bin/spomap.hashmap"));
		objout.writeObject(somap);
		objout.close();

		System.out.println("Finished.");

	} 

}
