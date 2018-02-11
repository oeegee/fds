package pe.exam.memory.store;

import java.io.Serializable;

import static java.lang.System.out;

/**
 * The KeyType class provides a key type for handling both non-composite and
 * composite keys. A key is a minimal set of attributes that can be used to
 * uniquely identify a tuple.
 *
 * @author Jeon DeukJin
 *
 */
public class KeyType implements Comparable<KeyType>, Serializable {
	/**
	 * default serialVersionUID
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Array holding the attribute values for a particular key
	 */
	@SuppressWarnings("rawtypes")
	private final Comparable[] key;

	/**
	 * Construct an instance of KeyType from a Comparable array.
	 * 
	 * @param key
	 *            the primary key
	 */
	@SuppressWarnings("rawtypes")
	public KeyType(Comparable[] key) {
		this.key = key;
	}

	/**
	 * Construct an instance of KeyType from a Comparable variable argument list.
	 * 
	 * @param _key
	 *            the primary key
	 */
	@SafeVarargs
	public KeyType(Comparable<?> key0, Comparable<?>... keys) {
		this.key = new Comparable[keys.length + 1];
		this.key[0] = key0;
		for (int i = 1; i < this.key.length; i++)
			this.key[i] = keys[i - 1];
	}

	/**
	 * Compare two keys (negative => less than, zero => equals, positive => greater
	 * than).
	 * 
	 * @param k
	 *            the other key (to compare with this)
	 * @return resultant integer that's negative, 0 or positive
	 */
	@SuppressWarnings("unchecked")
	public int compareTo(KeyType k) {
		for (int i = 0; i < this.key.length; i++) {
//			out.println("  this.key["+i+"].getClass():"+(this.key[i].getClass())+", k.key["+i+"].getClass():"+(k.key[i].getClass())+"\n");
			
			if(this.key[i].getClass() != k.key[i].getClass()) {
				return -1;
			}
			
//			out.println("  this.key["+i+"]:"+this.key[i]+", k.key["+i+"]:"+k.key[i]+"\n");
			if (this.key[i].compareTo(k.key[i]) < 0) {
				return -1;
			}else if (this.key[i].compareTo(k.key[i]) > 0) {
				return 1;
			}
		}
		return 0;
	}

	/**
	 * Determine whether two keys are equal (equals must agree with compareTo).
	 * 
	 * @param k
	 *            the other key (to compare with this)
	 * @return true if equal, false otherwise
	 */
	public boolean equals(KeyType k) {
		return compareTo(k) == 0;
	}

	/**
	 * Compute a hash code for this object (equal objects should produce the same
	 * hash code).
	 * 
	 * @return an integer hash code value
	 */
	public int hashCode() {
		int hashcode = 0;
		
		for (int i = 0; i < this.key.length; i++) {
			hashcode = hashcode + this.key[i].hashCode();
		}
		return hashcode;
	}

	/**
	 * Convert the key to a string.
	 * 
	 * @return the string representation of the key
	 */
	public String toString() {
		StringBuilder s = new StringBuilder("Key (");
		for (int i = 0; i < this.key.length; i++)
			s.append(" " + this.key[i]);
		return s.append(" )").toString();
	}

	/**
	 * The main method is used for testing purposes only.
	 * 
	 * @param args
	 *            the command-line arguments
	 */
	public static void main(String[] args) {
		KeyType key1 = new KeyType(new Comparable[] { "Star_Wars_2", 1980 });
		KeyType key2 = new KeyType(new Comparable[] { "Rocky", 1985 });
		KeyType key3 = new KeyType(new Comparable[] { "Star_Wars_2", 1980 });
		KeyType key4 = new KeyType(new Comparable[] { "Star_Wars_2", "1980" });
		
		out.println(key1.toString());
		
		out.println();
		out.println("Test the KeyClass");
		out.println();
		out.println("key1 = " + key1);
		out.println("key2 = " + key2);
		out.println("key3 = " + key3);
		out.println("key4 = " + key4);
		out.println();
		out.println("key1 < key2: " + (key1.compareTo(key2) < 0));
		out.println("key1 == key2: " + (key1.compareTo(key2) == 0));
		out.println("key1 > key2: " + (key1.compareTo(key2) > 0));
		out.println("key3 > key1: " + (key1.compareTo(key3) > 0));
		out.println("key1 == key4: " + (key1.compareTo(key4) > 0));
		out.println();
		out.println("key2 < key1: " + (key2.compareTo(key1) < 0));
		out.println("key2 == key1: " + (key2.compareTo(key1) == 0));
		out.println("key2 > key1: " + (key2.compareTo(key1) > 0));
		out.println();
		out.println("key1 < key3: " + (key1.compareTo(key3) < 0));
		out.println("key1 == key3: " + (key1.compareTo(key3) == 0));
		out.println("key1 > key3: " + (key1.compareTo(key3) > 0));
		out.println();
		out.println("key1.equals (key2): " + key1.equals(key2));
		out.println("key1.equals (key3): " + key1.equals(key3));
		out.println("key1.hashCode () == key2.hashCode (): " + (key1.hashCode() == key2.hashCode()));
		out.println("key1.hashCode () == key3.hashCode (): " + (key1.hashCode() == key3.hashCode()));
		out.println("key1.hashCode () == key4.hashCode (): " + (key1.hashCode() == key4.hashCode()));
	}
}