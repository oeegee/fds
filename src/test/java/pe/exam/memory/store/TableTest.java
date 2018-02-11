package pe.exam.memory.store;

import java.io.IOException;

public class TableTest {
	public static void main(String[] args) {

		Table movie = new Table("movie", "title year length genre studioName producerNo",
				"String Integer Integer String String Integer", "title year");

		Table cinema = new Table("cinema", "title year length genre studioName producerNo",
				"String Integer Integer String String Integer", "title year");

		Table movieStar = new Table("movieStar", "name address gender birthdate", "String String Character String",
				"name");

		Table starsIn = new Table("starsIn", "movieTitle movieYear starName", "String Integer String",
				"movieTitle movieYear starName");

		Table movieExec = new Table("movieExec", "certNo name address fee", "Integer String String Float", "certNo");

		Table studio = new Table("studio", "name address presNo", "String String Integer", "name");
		
		// for moview
		Comparable<?>[] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
		Comparable<?>[] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
		Comparable<?>[] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
		Comparable<?>[] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
		System.out.println();
		movie.insert(film0);
		movie.insert(film1);
		movie.insert(film2);
		movie.insert(film3);
		movie.print();

		Comparable<?>[] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
		System.out.println();
		cinema.insert(film2);
		cinema.insert(film3);
		cinema.insert(film4);
		// duplicate key
		System.out.println("\\n// --------------------- cinema.film4 - duplicate key[title year]");
		cinema.insert(film4);
		cinema.print();

		Comparable<?>[] star1 = { "Mark_Hamill", "Brentwood", 'M', "8/8/88" };
		Comparable<?>[] star0 = { "Carrie_Fisher", "Hollywood", 'F', "9/9/99" };
		Comparable<?>[] star2 = { "Harrison_Ford", "Beverly_Hills", 'M', "7/7/77" };
		System.out.println();
		movieStar.insert(star0);
		movieStar.insert(star1);
		movieStar.insert(star2);
		movieStar.print();

		Comparable<?>[] cast0 = { "Star_Wars", 1977, "Carrie_Fisher" };
		System.out.println();
		starsIn.insert(cast0);
		starsIn.print();

		Comparable<?>[] exec0 = { 9999, "S_Spielberg", "Hollywood", 10000.00 };
		System.out.println();
		movieExec.insert(exec0);
		movieExec.print();

		Comparable<?>[] studio0 = { "Fox", "Los_Angeles", 7777 };
		Comparable<?>[] studio1 = { "Universal", "Universal_City", 8888 };
		Comparable<?>[] studio2 = { "DreamWorks", "Universal_City", 9999 };
		System.out.println();
		studio.insert(studio0);
		studio.insert(studio1);
		studio.insert(studio2);
		studio.print();
		
		System.out.println("\n// --------------------- movie.save");
		movie.save();
		System.out.println("\n// --------------------- cinema.save");
		cinema.save();
		System.out.println("\n// --------------------- movieStar.save");
		movieStar.save();
		System.out.println("\n// --------------------- starsIn.save");
		starsIn.save();
		System.out.println("\n// --------------------- movieExec.save");
		movieExec.save();
		System.out.println("\n// --------------------- studio.save");
		studio.save();
		
		try {
			System.out.println("\n// --------------------- movie.load");
			Table movie0;
			movie0 = Table.load("movie");
			System.out.println("\n// --------------------- cinema.load");
			cinema = Table.load("cinema");
			System.out.println("\n// --------------------- movieStar.load");
			movieStar = Table.load("movieStar");
			System.out.println("\n// --------------------- starsIn.load");
			starsIn = Table.load("starsIn");
			System.out.println("\n// --------------------- movieExec.load");
			movieExec = Table.load("movieExec");
			System.out.println("\n// --------------------- studio.load");
			studio = Table.load("studio");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("\n// --------------------- movieStar.printIndex");
		movieStar.printIndex();

		// --------------------- project (select)

		System.out.println("\n// --------------------- project");
//		Table t_project = movie0.project("title year");
//		t_project.print();

		// --------------------- select predicate (where col1=val1 and col2=val2)
		System.out.println("\n// --------------------- select predicate");
		/*
		Predicate<T> largerThan(T reference)      : gt
		Predicate<T> largerThanOrEqual(T reference) : ge
		Predicate<T> smallerThan(T reference)       : lt
		Predicate<T> smallerThanOrEqual(T reference): le
		Predicate<T> equal(T reference)             : eq
		Predicate<T> notEqual(T reference)          : neq

		where <T extends Comparable<T>> and all methods use compareTo() internally

		sample usage:

		Collections2.filter(Arrays.asList(1,2,3,4), Predicates.largerThan(2))
		
		
		Comparable<?>[] t -> t[movie0.col("title")].equals("Star_Wars")
		Comparable<?>[] t -> {...} Predicate.test(Comparable<?>[])
		
		public Table select(Predicate<Comparable<?>[]> predicate) {
			rows = this.tuples.stream().filter(predicate).collect(Collectors.toList());
		
java.util.function.Predicate<Comparable<?>[]>
		
		
*/		
		Table t_select = movie.select(key -> key[movie.col("title")].equals("Star_Wars") &&
                ( key[movie.col("year")] ).equals(1977));
		t_select.print();
		
		System.out.println("\n// --------------------- select predicate ge(>=)");
		t_select = movie.select(tuple -> ((Integer) tuple[movie.col("year")]) >= 1977);
		t_select.print();
		
		System.out.println("\n// --------------------- select predicate gt(>)");
		t_select = movie.select(tuple -> ((Integer) tuple[1]) > 1977);
		t_select.print();
		// --------------------- indexed select (where key)

		System.out.println("\n// --------------------- indexed select");
		Table t_iselect = movieStar.select(new KeyType("Harrison_Ford"));
		t_iselect.print();
		t_iselect = movieStar.select(new KeyType("Carrie_Fisher"));
		t_iselect.print();

		// --------------------- union

		System.out.println("\n// --------------------- union");
		Table t_union = movie.union(cinema);
		t_union.print();

		// --------------------- minus

		System.out.println("\n// --------------------- minus");
		Table t_minus = movie.minus(cinema);
		t_minus.print();

		// --------------------- join

		System.out.println("\n// --------------------- join");
		Table t_join = movie.join("studioName", "name", studio);
		t_join.print();

		System.out.println("\n// --------------------- join");
		Table t_join2 = movie.indexJoin("title year", "title year", cinema);
		t_join2.print();
		
		// --------------------- indexed delete\
		System.out.println("\n// --------------------- indexed delete");
		movieStar.print();
//		System.out.println("  ==> delete result:" + movieStar.delete(new KeyType("Harrison_Ford")));
		movieStar.print();
		
		// --------------------- indexed update\
		System.out.println("\n// --------------------- indexed update");
//		int [] intArray = new int [] { 1, 2, 3, 4 };
		System.out.println("  ==> update result: "+ movieStar.update(new KeyType("Harrison_Ford"), new Comparable<?>[]{ "Harrison_Ford", "Beverly_Hills2", 'F', 7777 }));
		movieStar.print();
	
		// ---------------------  upsert\
		System.out.println("\n// --------------------- upsert");
//		int [] intArray = new int [] { 1, 2, 3, 4 };
//		System.out.println("  ==> upsert result1: "+ movieStar.upsert(new KeyType("Harrison_Ford"), new Comparable<?>[]{ "Harrison_Ford", "Beverly_Hills10", 'F', 7777 }));
		System.out.println("  ==> upsert result1: "+ movieStar.upsert(new Comparable<?>[]{ "Harrison_Ford2", "Beverly_Hills10", 'F', 8888 }));
		movieStar.print();
		System.out.println("  ==> upsert result2: "+ movieStar.upsert(new Comparable<?>[]{ "Harrison_Ford2", "Beverly_Hills20", 'M', 9999 }));
		movieStar.print();
		
		//	Table movieStar = new Table("movieStar", "name address gender birthdate", "String String Character String",

//		System.out.println("\n// --------------------- Object getValue");
//		System.out.println("  ==> Object getValue: "+ movieStar.getValue(new KeyType("Harrison_Ford"), "gender"));
	}
}
