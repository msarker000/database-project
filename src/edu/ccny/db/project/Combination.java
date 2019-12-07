package edu.ccny.db.project;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 
 * this class is used to find combination of a set of attribute
 * 
 * @author ayub
 *
 */
public class Combination {

	private List<Character> seed = new ArrayList<Character>();
	private Set<Set<Character>> subsets = new HashSet<>();

	public Combination(Set<Character> sets) {
		seed.addAll(sets);
		build();
	}

	private void build() {
		int numOfSubsets = (int) Math.pow(2, seed.size()); // OR Math.pow(2, list.size())
		// For i from 0 to 7 in case of [a, b, c],
		// we will pick 0(0,0,0) and check each bits to see any bit is set,
		// If set then element at corresponding position in a given Set need to be
		// included in a subset.
		for (int i = 0; i < numOfSubsets; i++) {
			Set<Character> subset = new HashSet<Character>();
			int mask = 1; // we will use this mask to check any bit is set in binary representation of
							// value i.
			for (int k = 0; k < seed.size(); k++) {
				if ((mask & i) != 0) { // If result is !=0 (or >0) then bit is set.
					subset.add(seed.get(k)); // include the corresponding element from a given set in a subset.
				}
				// check next bit in i.
				mask = mask << 1;
			}
			// add all subsets in final result.
			subsets.add(subset);
		}
	}
	/**
	 * gives a combination set of given length
	 * 
	 * @param len
	 * @return
	 */
	public Set<Set<Character>> gitCombinationOfLen(int len){
		Set<Set<Character>> subsetOfSpecifiedLen = subsets.stream().filter(set ->set.size()==len).collect(Collectors.toSet());
		return subsetOfSpecifiedLen;
	}
	
}
