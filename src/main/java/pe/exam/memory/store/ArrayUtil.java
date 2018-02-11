package pe.exam.memory.store;

import java.util.Arrays;

/**
 * @author Jeon DeukJin
 *
 */
class ArrayUtil {
	/**
	 * Concatenate two arrays of type T to form a new wider array.
	 *
	 * @param a
	 *            the first array
	 * @param b
	 *            the second array
	 * @return a wider array containing all the values from arr1 and arr2
	 */
	public static <T> T[] concat(T[] first, T[] second) {
		T[] result = Arrays.copyOf(first, first.length + second.length);
		System.arraycopy(second, 0, result, first.length, second.length);
		return result;
	}
}