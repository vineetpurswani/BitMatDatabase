package org.bitmat.extras;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;


public class StringSerialization {
	/** Read the object from Base64 string. */
	public static Object fromString( String s ) throws IOException, ClassNotFoundException {
		byte [] data = Base64.getDecoder().decode( s );
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
		Object o  = ois.readObject();
		ois.close();
		return o;
	}

	/** Write the object to a Base64 string. */
	public static String toString(Serializable o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream( baos );
		oos.writeObject(o);
		oos.close();
		return Base64.getEncoder().encodeToString(baos.toByteArray()); 
	}

	public static byte[] toByteArray(Serializable o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream( baos );
		oos.writeObject(o);
		oos.close();
		return baos.toByteArray();
	}
	
	public static Object fromByteArray(byte[] data) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
		Object o  = ois.readObject();
		ois.close();
		return o;
	}
	
	public static void main (String[] args) throws IOException, ClassNotFoundException {
		HashMap<String, ArrayList<Integer>> tpmap = new HashMap<String, ArrayList<Integer>>();
		tpmap.put("hey", new ArrayList(Arrays.asList(1,2,3)));
		tpmap.put("hey1", new ArrayList(Arrays.asList(10)));

		String stpmap = toString(tpmap);
		System.out.println(stpmap);
		System.out.println(((HashMap<String, ArrayList<Integer>>)fromString(stpmap)).get("hey1"));
	}
}
