package pe.exam.memory.store;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

/**
 * @author Jeon DeukJin
 *
 */
/**
 * @author Jeon DeukJin
 *
 */
public class Table implements Serializable {
	private static final Logger					log					= Logger.getLogger(Table.class.getName());

	private static final long					serialVersionUID	= 1L;

	/**
	 * Relative path for storage directory
	 */
	private static final String					DIR					= "store" + File.separator;

	/**
	 * Filename extension for database files
	 */
	private static final String					EXT					= ".dbf";

	/**
	 * print default limit for tuples
	 */
	private static final int LIMIT = 20;

	/**
	 * Table name.
	 */
	private final String						name;

	/**
	 * Array of attribute names.
	 */
	private final String[]						attribute;

	/**
	 * Array of attribute datatypes: a datatype may be integer types: Long, Integer,
	 * Short, Byte real types: Double, Float string types: Character, String
	 */
	private final Class<?>[]					datatype;

	/**
	 * Collection of tuples (data storage).
	 */
	private final List<Comparable<?>[]>			tuples;

	/**
	 * Primary key.
	 */
	private final String[]						key;

	/**
	 * Index into tuples (maps key to tuple number).
	 */
	private final Map<KeyType, Comparable<?>[]>	index;

	/**
	 * Construct an empty table from the meta-data specifications.
	 *
	 * @param name
	 *            the name of the relation
	 * @param _attribute
	 *            the string containing attributes names
	 * @param _datatype
	 *            the string containing attribute datatypes (data types)
	 * @param _key
	 *            the primary key
	 */
	public Table(String name, String[] attribute, Class<?>[] datatype, String[] key) {
		this.name = name;
		this.attribute = attribute;
		this.datatype = datatype;
		this.key = key;
		this.tuples = new ArrayList<>(10000);
		this.index = new ConcurrentSkipListMap<>();
	}

	/**
	 * Construct a table from the meta-data specifications and data in _tuples list.
	 *
	 * @param name
	 *            the name of the relation
	 * @param attribute
	 *            the string containing attributes names
	 * @param datatype
	 *            the string containing attribute datatypes (data types)
	 * @param key
	 *            the primary key
	 * @param tuple
	 *            the list of tuples containing the data
	 */
	public Table(String name, String[] attribute, Class<?>[] datatype, String[] key, List<Comparable<?>[]> tuples) {
		this.name = name;
		this.attribute = attribute;
		this.datatype = datatype;
		this.key = key;
		this.tuples = tuples;
		this.index = new ConcurrentSkipListMap<>();
	}

	/**
	 * Construct an empty table from the raw string specifications.
	 *
	 * @param name
	 *            the name of the relation
	 * @param attributes
	 *            the string containing attributes names
	 * @param datatypes
	 *            the string containing attribute datatypes (data types)
	 */
	public Table(String name, String attributes, String datatypes, String key) {
		this(name, attributes.split(" "), findClass(datatypes.split(" ")), key.split(" "));
		 log.trace ("DDL> create table " + name + " (" + attributes + ")");
	} // constructor

	/**
	 * Project the tuples onto a lower dimension by keeping only the given
	 * attributes. Check whether the original key is included in the projection.
	 *
	 * #usage movie.project ("title year studioNo")
	 *
	 * @param attributes
	 *            the attributes to project onto
	 * @return a table of projected tuples
	 */
	public Table project(String attributes) {
		log.debug("RA> " + name + ".project (" + attributes + ")");

		String[] attrs = attributes.split(" ");
		Class<?>[] colDatatype = extractDom(match(attrs), this.datatype);
		String[] newKey = (Arrays.asList(attrs).containsAll(Arrays.asList(this.key))) ? this.key : attrs;

		List<Comparable<?>[]> rows = new ArrayList<>();

		// extract the columns needed from the tuples and add them to the list rows
		this.tuples.stream().forEach((tuple) -> {
			rows.add(this.extract(tuple, attrs));
		});

		return new Table(this.name+"_project", attrs, colDatatype, newKey, rows);
	} // project

	/**
	 * get key. column array of table.
	 * @return
	 */
	public String[] getKey() {
		return this.key;
	}
	
	
	/**
	 * Select the tuples satisfying the given predicate (Boolean function).
	 *
	 * #usage movie.select (t -> t[movie.col("year")].equals (1977))
	 *
	 * @param predicate
	 *            the check condition for tuples
	 * @return a table with tuples satisfying the predicate
	 */
	public Table select(Predicate<Comparable<?>[]> predicate) {
		if(log.isTraceEnabled()) {
			log.trace("RA> " + this.name + ".select (" + predicate + ")");
		}
		List<Comparable<?>[]> rows = null;

		// data is in tuples
		try {
			// try to run the tuples through a stream
			// filter based on the predicate
			// and add to a list
			// may not need to b rows=... and can add it to the list in the collect
			// statement
			rows = this.tuples.stream().filter(predicate).collect(Collectors.toList());
		} catch (Exception e) {
			log.error("Error in select(Predicate <Comparable []> predicate)");
			e.printStackTrace();
		}
		return new Table(this.name+"_select", this.attribute, this.datatype, this.key, rows);
	}

	/**
	 * Select the tuples satisfying the given key predicate (key = value). Use an
	 * index (Map) to retrieve the tuple with the given key value.
	 *
	 * @param key
	 *            the given key value
	 * @return a table with the tuple satisfying the key predicate
	 */
	public Table select(KeyType keyType) {
		// Michael wrote this amazingly awesome code as well (other than what Dr. Miller
		// wrote)
		if(log.isTraceEnabled()) {
			log.trace("RA> " + this.name + ".select (" + keyType + ")");
		}
		List<Comparable<?>[]> rows = null;

		try {
			// getting the tuple [] that satisfies the key
			Comparable<?>[] tempTuples = this.index.get(keyType);
			if (tempTuples != null) {
				// if the key exists, initialize the List rows and add the tuple [] to rows
				rows = new ArrayList<>();
				rows.add(tempTuples);
			}
		} catch (Exception e) {
			// potentially unexpected exception
			// same jazz as above with method name and stack trace
			log.error("Error in select(KeyType keyType)");
			e.printStackTrace();
		}
		return new Table(this.name+"_select", this.attribute, this.datatype, this.key, rows);
	}

	/**
	 * Union this table and table2. Check that the two tables are compatible.
	 *
	 * #usage movie.union (show)
	 *
	 * @param table2
	 *            the rhs table in the union operation
	 * @return a table representing the union
	 */
	public Table union(Table table2) {
		log.debug("RA> " + this.name + ".union (" + table2.name + ")");
		if (!compatible(table2))
			return null;

		Table resultTable = new Table(this.name+"_union_"+table2.name, this.attribute, this.datatype, this.key);

		// insert tuples of current table
		this.tuples.stream().forEach((tuple) -> {
			resultTable.insert(tuple);
		});

		// checks table2 for unique tuples
		table2.tuples.stream().forEach((tuple1) -> {
			boolean exists = false;
			for (Comparable<?>[] tuple : this.tuples) {
				if (tuple1 == tuple) {
					exists = true;
				}
			}
			// adds tuples to the resultTable
			if (!exists) {
				resultTable.insert(tuple1);
			}
		});

		return resultTable;
	}

	/**
	 * Take the difference of this table and table2. Check that the two tables are
	 * compatible.
	 *
	 * #usage movie.minus (show)
	 *
	 * @param table2
	 *            The rhs table in the minus operation
	 * @return a table representing the difference
	 */
	public Table minus(Table table2) {
		log.debug("RA> " + this.name + ".minus (" + table2.name + ")");
		if (!compatible(table2))
			return null;

		Table resultTable = new Table(this.name + "_minus", this.attribute, this.datatype, this.key);

		for (Comparable<?>[] tuple : this.tuples) {
			boolean exists = false;
			for (Comparable<?>[] tuple1 : table2.tuples) {
				// checks if the tuple exists in table 2
				if (tuple == tuple1) {
					exists = true;
					break;
				}
			}
			// if the tuple doesn't exist in table 2, it's added to the resultTable
			if (!exists) {
				resultTable.insert(tuple);
			}
		}

		return resultTable;
	}

	/**
	 * Join this table and table2 by performing an equijoin. Tuples from both tables
	 * are compared requiring attributes1 to equal attributes2. Disambiguate
	 * attribute names by append "2" to the end of any duplicate attribute name.
	 *
	 * #usage movie.join ("studioNo", "name", studio) #usage movieStar.join ("name
	 * == s.name", starsIn)
	 *
	 * @param attribute1
	 *            the attributes of this table to be compared (Foreign Key)
	 * @param attribute2
	 *            the attributes of table2 to be compared (Primary Key)
	 * @param table2
	 *            the rhs table in the join operation
	 * @return a table with tuples satisfying the equality predicate
	 */
	public Table join(String attributes1, String attributes2, Table table2) {
		if(log.isTraceEnabled()) {
			log.trace("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", " + table2.name + ")");
		}
		String[] t_attrs = attributes1.split(" ");
		String[] u_attrs = attributes2.split(" ");

		// see if the input was formatted properly and if not we print an error
		if (t_attrs.length != u_attrs.length) {
			log.debug("your attributes were crap and did not have the same number for either table.");
			return null;
		}

		// creates a length value for datatypes and attributes to create a temp result
		// table
		int attrLen = this.attribute.length + table2.attribute.length;
		int domLen = this.datatype.length + table2.datatype.length;
		// int keyLen = this.key.length + table2.key.length;

		// use the new lengths and create temp attr array and temp datatype array
		String[] jtAttrs = new String[attrLen];
		Class<?>[] jtDom = new Class[domLen];
		// String [] jtKey = new String[keyLen];

		// adds the attrs from the first table to our new attribute list
		int attrPos = 0;
		for (String attribute1 : this.attribute) {
			jtAttrs[attrPos] = attribute1;
			attrPos++;
		}
		// this one fills the new attr arraylist with the attributes from the second
		// table
		// ,but renames them if they are the same as one in the first table
		for (int k = 0; k < table2.attribute.length; k++) {
			jtAttrs[attrPos] = table2.attribute[k] + "2";
			attrPos++;
		}

		// fill the new datatype arraylist with the datatypes from the first and second
		// table
		int domPos = 0;
		for (Class<?> datatype1 : this.datatype) {
			jtDom[domPos] = datatype1;
			domPos++;
		}
		for (Class<?> datatype1 : table2.datatype) {
			jtDom[domPos] = datatype1;
			domPos++;
		}

		// create our temporary result table using the combined attribute and datatype
		// arrays
//		Table result = new Table((name + counter++), jtAttrs, jtDom, key);
		Table result = new Table((this.name + "_join_"+ table2.name), jtAttrs, jtDom, this.key);

		boolean addT = false;

		for (int i = 0; i < this.tuples.size(); i++) {
			for (int j = 0; j < table2.tuples.size(); j++) {
				Comparable<?>[] tuple1 = this.tuples.get(i);
				Comparable<?>[] tuple2 = table2.tuples.get(j);
				addT = false;
				for (int m = 0; m < t_attrs.length; m++) {
					if (tuple1[this.col(t_attrs[m])] == tuple2[table2.col(u_attrs[m])]) {
						addT = true;
					} else {
						break;
					}
				}
				if (addT) {
					result.insert(ArrayUtil.concat(tuple1, tuple2));
				}
			}
		}

		return result;
	}

	/**
	 *
	 * @param table2
	 *            the rhs table in the join operation
	 * @return a table with tuples satisfying the equality predicate
	 */
	public Table indexJoin(String attributes1, String attributes2, Table table2) {
		
		if(log.isDebugEnabled()) {
			log.debug("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", " + table2.name + ")");
		}
		
		String[] t_attrs = attributes1.split(" ");
		String[] u_attrs = attributes2.split(" ");

		// see if the input was formatted properly and if not we print an error
		if (t_attrs.length != u_attrs.length) {
			log.warn("your attributes were crap and did not have the same number for either table.");
			return null;
		}

		// creates a length value for datatypes and attributes to create a temp result
		// table
		int attrLen = this.attribute.length + table2.attribute.length;
		int domLen = this.datatype.length + table2.datatype.length;

		// use the new lengths and create temp attr array and temp datatype array
		String[] jtAttrs = new String[attrLen];
		Class<?>[] jtDom = new Class[domLen];

		// adds the attrs from the first table to our new attribute list
		int attrPos = 0;
		for (String attribute1 : this.attribute) {
			jtAttrs[attrPos] = attribute1;
			attrPos++;
		}
		// this one fills the new attr arraylist with the attributes from the second
		// table
		// ,but renames them if they are the same as one in the first table
		for (int k = 0; k < table2.attribute.length; k++) {
			jtAttrs[attrPos] = table2.attribute[k] + "2";
			attrPos++;
		}

		// fill the new datatype arraylist with the datatypes from the first and second
		// table
		int domPos = 0;
		for (Class<?> datatype1 : this.datatype) {
			jtDom[domPos] = datatype1;
			domPos++;
		}
		for (Class<?> datatype1 : table2.datatype) {
			jtDom[domPos] = datatype1;
			domPos++;
		}

		// create our temporary result table using the combined attribute and datatype
		// arrays
		Table result = new Table((this.name + "_indexJoin_" + table2.name), jtAttrs, jtDom, this.key);

		for (int i = 0; i < this.tuples.size(); i++) {
			Comparable<?>[] tuple1 = this.tuples.get(i);
			Comparable<?>[] tempKeys = new Comparable<?>[t_attrs.length];
			for (int j = 0; j < t_attrs.length; j++) {
				Comparable<?> tupVal = tuple1[this.col(t_attrs[j])];
				tempKeys[j] = tupVal;
			}
			Comparable<?>[] tuple2 = table2.index.get(new KeyType(tempKeys));
			if (tuple2 != null) {
				result.insert(ArrayUtil.concat(tuple1, tuple2));
			}
		}
		return result;
	}

	/**
	 * Return the column position for the given attribute name.
	 *
	 * @param attr
	 *            the given attribute name
	 * @return a column position
	 */
    public int col(String attr)
    {
        for (int i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        }
        return -1;
    }
    
    /**
     * get column value 
     * 
     * @param key
     * @param attr
     * @return 
     * @return 
     * @return
     */
	public <T> Object getValue(KeyType keyType, String attr) {
		if (log.isTraceEnabled()) {
			log.trace("RA> " + this.name + ".select (" + keyType + ")");
		}

		Object value = null;

		try {
			// getting the tuple [] that satisfies the key
			Comparable<?>[] tempTuples = this.index.get(keyType);
			if (tempTuples != null) {
				// if the keyVal exists, initialize the List rows and add the tuple [] to rows
				value = tempTuples[this.col(attr)];
				log.trace("Object getValue - type: "+value.getClass().getSimpleName());
			}
		} catch (Exception e) {
			// potentially unexpected exception
			// same jazz as above with method name and stack trace
			log.error("Error in select(KeyType keyVal)");
			e.printStackTrace();
		}
    	return value;
    }

	/**
	 * Insert a tuple to the table.
	 *
	 * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
	 *
	 * @param tup
	 *            the array of attribute values forming the tuple
	 * @return whether insertion was successful
	 */
	public boolean insert(Comparable<?>[] insertTuple) {
		boolean result = false;

		if(log.isTraceEnabled()) {
			log.trace("DML> insert into " + name + " values ( " + Arrays.toString(insertTuple) + " )");
		}
		
		try {
			if (typeCheck(insertTuple)) {
//				int tupleIndex = this.tuples.indexOf(tuple);
				
				if( this.index.containsValue(insertTuple) ) {
					log.error("insert tuple duplicated. table ["+this.name+"].");
					return false;
				}
				// add tuple
				tuples.add(insertTuple);
				
				Comparable<?>[] keyVal = new Comparable<?>[key.length];
				int[] cols = match(key);
				for (int j = 0; j < keyVal.length; j++) {
					keyVal[j] = insertTuple[cols[j]];
				}
				// add key, tuple
				index.put(new KeyType(keyVal), insertTuple);
				result = true;
			}else {
				log.error("does not match datatype for table["+this.name+"].");
			}
			
		} catch (Exception e) {
			log.error("Error in insert(Comparable<?>[] tup):" + insertTuple);
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * insert list of tuple to the table for performance.
	 * @param tups
	 * @return
	 */
	public int insert(List<Comparable<?>[]> insertTuples) {
		int result = 0;
		
		try {
			Comparable<?>[] tup;
			
			for(int i=0; i< insertTuples.size(); i++) {
				tup = insertTuples.get(i);
				
				try {
					if (typeCheck(tup)) {
						if(this.index.containsValue(tup)) {
							log.error("insert tuple duplicated. table ["+this.name+"]. list.index("+i+")");
							continue;
						}
						
						tuples.add(tup);
						Comparable<?>[] keyVal = new Comparable<?>[key.length];
						int[] cols = match(key);
						for (int j = 0; j < keyVal.length; j++) {
							keyVal[j] = tup[cols[j]];
						}
						index.put(new KeyType(keyVal), tup);
						result += 1;
					}else {
						log.warn("type check: invalid type this datatype. index of list: "+ insertTuples.indexOf(tup) +", the tuple: ("+tup+")");
					}
				}catch(Exception e) {
					log.warn("Error in insert (List<Comparable<?>[]> tups)");
				}
			}
			
			
		} catch (Exception e) {
			log.error("Error in insert (List<Comparable<?>[]> tups)");
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * list indexed delete for performance.
	 * 
	 * @param keyVal
	 * @return
	 */
	public int delete(List<KeyType> deleteKeys) {
		log.trace("RA> " + this.name + ".delete (" + deleteKeys + ")");
		
		int result = 0;
		try{
			
			for(KeyType keyVal: deleteKeys) {
				Comparable<?>[] tempTuples = this.index.get(keyVal);
				
				if (tempTuples != null) {
					this.tuples.remove(tempTuples);
					this.index.remove(keyVal);
					result += 1;
				}
			}
			
		} catch (Exception e) {
			log.error("Error in delete(List<KeyType> keyVals)");
			e.printStackTrace();
		}
		return result;
	}
	
	/**
		 * indexed delete
		 * 
		 * @param keyVal
		 * @return
		 */
		public boolean delete(KeyType keyType) {
			log.trace("RA> " + this.name + ".delete (" + keyType + ")");
			
			boolean result = false;
			try{
				Comparable<?>[] tempTuples = this.index.get(keyType);
				
				if (tempTuples != null) {
					this.tuples.remove(tempTuples);
					this.index.remove(keyType);
					result = true;
				}
			} catch (Exception e) {
				log.error("Error in delete(KeyType key)");
				e.printStackTrace();
			}
			return result;
		}

	/**
	 * indexed update
	 * 
	 * @param keyVal
	 * @param tup
	 * @return
	 */
	public boolean update(KeyType keyType, Comparable<?>[] updateTuple) {
		log.trace("RA> " + this.name + ".update (" + keyType + ")");
		
		boolean result = false;
		try{
			if (typeCheck(updateTuple)) {
				Comparable<?>[] orgTuple = this.index.get(keyType);
				int tupleIndex = this.tuples.indexOf(orgTuple);
				
				if (orgTuple != null && tupleIndex > -1) {
					this.tuples.set(tupleIndex, updateTuple);
					this.index.replace(keyType, updateTuple);
					result = true;
				}
			} 
		} catch (Exception e) {
			log.error("Error in update(KeyType keyVal)");
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * update tuples.
	 * 
	 * @param updateTuples
	 * @return
	 */
	public int update(Map<KeyType, Comparable<?>[]> updateTuples) {
		log.trace("RA> " + this.name + ".update (" + updateTuples + ")");
		
		int result = 0;
		try{
			Iterator<KeyType> iter = updateTuples.keySet().iterator();
			
			KeyType keyType;
			Comparable<?>[] updateTuple;
			
			while(iter.hasNext()) {
				keyType = (KeyType) iter.next();
				updateTuple = updateTuples.get(keyType);
				
				if (typeCheck(updateTuple)) {
					Comparable<?>[] orgTuple = this.index.get(keyType);
					int tupleIndex = this.tuples.indexOf(orgTuple);
					
					//Find the position of the origin tuple and replace it with a new tuple.
					if (orgTuple != null && tupleIndex > -1) {
						this.tuples.set(tupleIndex, updateTuple);
						this.index.replace(keyType, updateTuple);
						result += 1;
					}
				}
			}
		} catch (Exception e) {
			log.error("Error in delete(KeyType keyVal)");
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * upsert : exist in table ==> update not exist in table ==> insert
	 * 
	 * @param keyVal
	 * @param updateTuple
	 * @return
	 */
	public boolean upsert(Comparable<?>[] upsertTuple) {
		log.trace("RA> " + this.name + ".upsert (" + upsertTuple + ")");
		
		boolean result = false;
		try{
			
			if (typeCheck(upsertTuple)) {
				// generate keyval
				Comparable<?>[] keyVal = new Comparable<?>[this.key.length];
				int[] cols = match(this.key);
				for (int i = 0; i < keyVal.length; i++) {
					keyVal[i] = upsertTuple[cols[i]];
				}
				KeyType keyType = new KeyType(keyVal);

				// add key, tuple
				Comparable<?>[] orgTuple= this.index.get(keyType);
				int tupleIndex = this.tuples.indexOf(orgTuple);
				
				// update
				if (orgTuple != null && tupleIndex > -1) {
					this.tuples.set(tupleIndex, upsertTuple);
					this.index.replace(keyType, upsertTuple);
					result = true;
				// insert
				}else {
					this.insert(upsertTuple);
					result = true;
				}
			} 
		} catch (Exception e) {
			log.error("Error in upsert(KeyType key)");
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Get the name of the table.
	 *
	 * @return the table's name
	 */
	public String getName() {
		return name;
	} // getName

	/**
	 * Get table row count
	 * @return count
	 */
	public int count() {
		return (null != this.tuples ? this.tuples.size() : 0);
	}
	
	/**
	 * Get table row count
	 * @return count
	 */
	public boolean isExist() {
		return (this.count() > 0 ? true : false);
	}
	
	/**
	 * Print system out this table
	 */
	public void print(){
		System.out.println(this.printString());
	}
	
	public void print(int limit){
		System.out.println(this.printString(limit));
	}
	
	/**
	 * PrintString this table.
	 * @return printString
	 */
	public String printString(){
		return this.printString(Table.LIMIT);
	}
	
	/**
	 * printString this table until limit.
	 * @param limit
	 * @return printString
	 */
	public String printString(int limit){
		
		int maxLimit = this.LIMIT;
		
		int columnSpace = 20;
		String line = String.format("%"+columnSpace+"s", "-").replace(" ", "-");
		
		if(null != this.tuples) {
			maxLimit = this.tuples.size();
		}
		
		if(limit <= 0) {
			limit = maxLimit;
		}else {
			if(limit > maxLimit) {
				limit = maxLimit;
			}
		}
		
		StringBuilder sb = new StringBuilder()
				.append("\n Table [" + name + "]\n")
				.append( (this.index == null)? " ( index.size: 0," : " ( index.size: " + this.index.size())
				.append( (this.tuples == null)? ", tuples.size: 0," :", tuples.size: " + this.tuples.size())
				.append( (this.attribute == null)? ", attribute.length: 0," : ", attribute.length: " + this.attribute.length + " )\n")
				.append("|-");

		for (int i = 0; i < this.attribute.length; i++) {
			sb.append(line);
		}
		sb.append("-|\n").append("| ");
		for (String a : this.attribute) {
			sb.append(String.format("%"+columnSpace+"s", a));
		}
		sb.append(" |\n");  // print columns
		
		sb.append("| ");
		for (Class<?> a : this.datatype) {
			sb.append(String.format("%"+columnSpace+"s", "["+a.getSimpleName()+"]" ));
		}
		sb.append(" |\n")  // print datatype
		
		.append("|-");
		
		for (int i = 0; i < this.attribute.length; i++) {
			sb.append(line);
		}
		sb.append("-|\n"); // print header end.
		if (null != this.tuples) {
			Comparable<?>[] tup;
			for(int i=0; i<limit; i++) {
//			for (Comparable<?>[] tup : this.tuples) {
				tup = this.tuples.get(i);
				sb.append("| ");
				for (Comparable<?> attr : tup) {
					sb.append(String.format("%"+columnSpace+"s", attr));
				}
				sb.append(" |\n");
			}
		}
		sb.append("|-");
		for (int i = 0; i < this.attribute.length; i++) {
			sb.append(line);
		}
		sb.append("-|\n limit: "+limit+" row.\n");
		
		return sb.toString();
	}

	/**
	 * Print this table's index (Map).
	 */
	public void printIndex() {
		System.out.println("\n Index for " + this.name);
		System.out.println("------------------------------------------------------------");
		for (Entry<KeyType, Comparable<?>[]> e : this.index.entrySet()) {
			System.out.println(e.getKey() + " -> " + Arrays.toString(e.getValue()));
		} // for
		System.out.println("------------------------------------------------------------");
	}

	/**
	 * Load the table with the given name into memory.
	 *
	 * @param name
	 *            the name of the table to load
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static Table load(String name) throws IOException, ClassNotFoundException {
		Table tab = null;
		ObjectInputStream ois = null;
		GZIPInputStream gis = null;
		BufferedInputStream bis = null;
		FileInputStream fis = null;
		
		try {
			String dir = DIR + name + EXT;
			
			File path = new File(dir);
			if(!path.exists()) {
				log.warn("table file does not exist! fine name: ["+dir+"]. return null.");
				return null;
			}
			
			fis = new FileInputStream(dir);
			gis = new GZIPInputStream(fis);
			bis = new BufferedInputStream(gis, 2048);
			ois = new ObjectInputStream(bis);
			
			tab = (Table) ois.readUnshared();
			
		} catch (IOException ex) {
			log.error("load: IO Exception");
			ex.printStackTrace();
			throw ex;
		} catch (ClassNotFoundException ex) {
			log.error("load: Class Not Found Exception");
			ex.printStackTrace();
			throw ex;
		} finally {
			try {
				ois.close();
				bis.close();
				gis.close();
				fis.close();
				
				ois = null;
				bis = null;
				gis = null;
				fis = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return tab;
	}

	/**
	 * Save this table in a file. Default DIR is 'store'
	 */
	public void save() {
		save(Table.DIR);
	}
	
	/**
	 * save table. but, file already exist overwrite it.
	 * file name is '$dir/${table name}.dbf'
	 * @param path
	 */
	public void save(String dir) {
		
		FileOutputStream fos = null;
		GZIPOutputStream gos = null;
		BufferedOutputStream bos = null;
		ObjectOutputStream oos = null;
		
		try {
			File path = new File(dir);
			if(path.exists()) {
				if(!path.isDirectory()) {
					path.delete();
					path.mkdirs();
				}
			}else {
				path.mkdirs();
			}
			// create file
			fos = new FileOutputStream(DIR + name + EXT);
			gos = new GZIPOutputStream(fos);
			bos = new BufferedOutputStream(gos, 2048);
			oos = new ObjectOutputStream(bos);
			
			oos.writeUnshared(this);
			oos.flush();
			oos.reset();
			
		} catch (IOException ex) {
			log.debug("save: IO Exception");
			ex.printStackTrace();
		} finally {
			
			try {
				oos.close();
				bos.close();
				gos.close();
				fos.close();
				
				oos = null;
				bos = null;
				gos = null;
				fos = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * Determine whether the two tables (this and table2) are compatible, i.e., have
	 * the same number of attributes each with the same corresponding datatype.
	 *
	 * @param table2
	 *            the rhs table
	 * @return whether the two tables are compatible
	 */
	@SuppressWarnings("unused")
	private boolean compatible(final Table table2) {
		if (datatype.length != table2.datatype.length) {
			log.debug("compatible ERROR: table have different arity");
			return false;
		}
		for (int j = 0; j < datatype.length; j++) {
			if (datatype[j] != table2.datatype[j]) {
				log.debug("compatible ERROR: tables disagree on datatype " + j);
				return false;
			}
		}
		return true;
	}

	/**
	 * Match the column and attribute names to determine the datatypes.
	 *
	 * @param column
	 *            the array of column names
	 * @return an array of column index positions
	 */
	private int[] match(String[] column) {
		int[] colPos = new int[column.length];

		for (int j = 0; j < column.length; j++) {
			boolean matched = false;
			for (int k = 0; k < attribute.length; k++) {
				if (column[j].equals(attribute[k])) {
					matched = true;
					colPos[j] = k;
				}
			}
			if (!matched) {
				log.debug("match: datatype not found for " + column[j]);
			}
		}

		return colPos;
	}

	/**
	 * Extract the attributes specified by the column array from tuple t.
	 *
	 * @param t
	 *            the tuple to extract from
	 * @param column
	 *            the array of column names
	 * @return a smaller tuple extracted from tuple t
	 */
	private Comparable<?>[] extract(Comparable<?>[] t, String[] column) {
		Comparable<?>[] tup = new Comparable<?>[column.length];
		int[] colPos = match(column);
		for (int j = 0; j < column.length; j++)
			tup[j] = t[colPos[j]];
		return tup;
	}

	/**
	 * Check the size of the tuple (number of elements in list) as well as the type
	 * of each value to ensure it is from the right datatype.
	 *
	 * @param t
	 *            the tuple as a list of attribute values
	 * @return whether the tuple has the right size and values that comply with the
	 *         given datatypes
	 * @throws Exception 
	 */
	private boolean typeCheck(Comparable<?>[] t) throws InputMismatchException {
		if (t.length != attribute.length)
			return false;

		for (int i = 0; i < datatype.length; i++) {
			// checks t's type and compares it to the current datatype
			if (t[i].getClass().equals(datatype[i].getClass())) {
//				return false;
				throw new InputMismatchException("input data type does not match. input.datatype["+i+"].getClass()["+datatype[i].getClass().getSimpleName()+"] vs defined ["+t[i].getClass().getSimpleName()+"]");
			}
		}
		return true;
	}

	/**
	 * Find the classes in the "java.lang" package with given names.
	 *
	 * @param className
	 *            the array of class name (e.g., {"Integer", "String"})
	 * @return an array of Java classes
	 */
	private static Class<?>[] findClass(String[] className) {
		Class<?>[] classArray = new Class[className.length];

		for (int i = 0; i < className.length; i++) {
			try {
				classArray[i] = Class.forName("java.lang." + className[i]);
			} catch (ClassNotFoundException ex) {
				log.debug("findClass: " + ex);
			}
		}
		return classArray;
	}

	/**
	 * Extract the corresponding datatypes.
	 *
	 * @param colPos
	 *            the column positions to extract.
	 * @param group
	 *            where to extract from
	 * @return the extracted datatypes
	 */
	private Class<?>[] extractDom(int[] colPos, Class<?>[] group) {
		Class<?>[] obj = new Class[colPos.length];

		for (int j = 0; j < colPos.length; j++) {
			obj[j] = group[colPos[j]];
		}
		return obj;
	}
}