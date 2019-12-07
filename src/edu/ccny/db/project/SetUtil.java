package edu.ccny.db.project;


import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This class is a utils for all set operations
 * 
 * @author ayub
 *
 */

public class SetUtil {

	/**
	 * returns union of set A and B
	 * 
	 * @param setA
	 * @param setB
	 * @return set
	 */
	
	public static <T> Set<T> union(Set<T> setA, Set<T> setB) {
		Set<T> tmp = new TreeSet<T>(setA);
		tmp.addAll(setB);
		return tmp;
	}

	/**
	 * returns intersection of set A and B
	 * 
	 * @param setA
	 * @param setB
	 * @return set
	 */
	
	public static <T> Set<T> intersection(Set<T> setA, Set<T> setB) {
		Set<T> tmp = new TreeSet<T>();
		for (T x : setA)
			if (setB.contains(x))
				tmp.add(x);
		return tmp;
	}

	/**
	 * returns set difference of set A and B
	 * 
	 * @param setA
	 * @param setB
	 * @return
	 */
	public static <T> Set<T> difference(Set<T> setA, Set<T> setB) {
		Set<T> tmp = new TreeSet<T>(setA);
		tmp.removeAll(setB);
		return tmp;
	}

	/**
	 * checks set B is subset of A
	 * 
	 * @param setA
	 * @param setB
	 * @return true or false
	 */
	public static <T> boolean isSubset(Set<T> setA, Set<T> setB) {
		if(setB.isEmpty()){
			return false;
		}
		return setA.containsAll(setB);
	}

	/**
	 * checks set A is super set of B
	 * 
	 * @param setA
	 * @param setB
	 * @return true or false
	 */
	public static <T> boolean isSuperSet(Set<T> setA, Set<T> setB) {
		return setA.containsAll(setB);
	}

	/**
	 * checks where SetA matches with setB
	 * 
	 * @param setA
	 * @param setB
	 * @return true or false
	 */
	public static <T> boolean isMatched(Set<T> setA, Set<T> setB) {
		return !setA.isEmpty() && !setB.isEmpty() && setA.size() == setB.size() && setA.containsAll(setB);
	}

	/**
	 * gets a set of character from string
	 * 
	 * @param setStr
	 * @return set of character
	 */
	public static Set<Character> getSet(String setStr) {
		Set<Character> set = new TreeSet<Character>();
		char[] chars = setStr.toCharArray();
		for (char ch : chars) {
			set.add(ch);
		}
		return set;
	}

	public static Set<Character> getSetFromString(String string) {
		Set<Character> treeSet = new TreeSet<>();
		for (Character ch : string.toCharArray()) {
			treeSet.add(ch);
		}
		return treeSet;
	}
	
	public static String setToString(Set<Character> characters){
		return characters.stream().map(ch -> String.valueOf(ch)).collect(Collectors.joining());
	}

}
