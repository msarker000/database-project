package edu.ccny.db.project;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * this class is the responsible to find normal form, closures,
 * 
 * 
 * @author ayub
 *
 */
public class DBUtil {
	/**
	 * finds the normal form of a relation
	 * 
	 * @param table
	 */
	public static NormalForm findNormalForm(Table table) {
		NormalForm relationNormalForm = NormalForm.NONE;
		for (FD fd : table.getOriginalFDs()) {
			if (DBUtil.isBCNF(fd, table.getKeys())) {
				fd.setNormalForm(NormalForm.BCNF);
			} else if (DBUtil.is3NF(fd, table.getKeys())) {
				fd.setNormalForm(NormalForm.THIRD_NF);
			} else if (DBUtil.is2NF(fd, table.getKeys())) {
				fd.setNormalForm(NormalForm.SECOND_NF);
			} else {
				fd.setNormalForm(NormalForm.FIRST_NF);
			}
			if (fd.getNormalForm().getPriority() < relationNormalForm.getPriority()) {
				relationNormalForm = fd.getNormalForm();
			}
		}
		table.setNormalForm(relationNormalForm);
		return relationNormalForm;
	}

	/**
	 * finds closures of a give seed
	 * 
	 * @param functionDependencies
	 * @param seeds
	 * @return
	 */
	public static Set<Character> findClosure(Set<FD> functionDependencies, Set<Character> seeds) {

		Set<Character> closureSet = new TreeSet<Character>();
		closureSet.addAll(seeds);

		// reset fd before start
		for (FD fd : functionDependencies) {
			fd.isTaken = false;
		}

		boolean isChanged = true;
		while (isChanged) {
			isChanged = false;
			for (FD fd : functionDependencies) {
				if (!fd.isTaken && SetUtil.isSubset(closureSet, fd.getAttributesOfLhsOfFD())) {
					closureSet.addAll(fd.getAttributesOfRhsOfFD());
					fd.isTaken = true;
					isChanged = true;
				}
			}
		}

		return closureSet;
	}

	/**
	 * finds the keys of given relation
	 * 
	 * @param table
	 * @return 
	 */
	public static Set<Set<Character>> findKeys(Table table) {
		Set<Character> allAttributesOnBothSideOfFDs = table.getConvertedNonTrivialFDs().stream()
				.map(x -> x.getAllAttributesOfbothSide()).flatMap(x -> x.stream()).collect(Collectors.toSet());
		Set<Character> attributesOnLhsSideOfFDs = table.getConvertedNonTrivialFDs().stream()
				.map(x -> x.getAttributesOfLhsOfFD()).flatMap(x -> x.stream()).collect(Collectors.toSet());
		Set<Character> attributesOnRhsSideOfFDs = table.getConvertedNonTrivialFDs().stream()
				.map(x -> x.getAttributesOfRhsOfFD()).flatMap(x -> x.stream()).collect(Collectors.toSet());

		// 1. Find the attributes that are neither on the left and right side
		Set<Character> attributesNotOnLhsOrRhsOfFDs = SetUtil.difference(table.getAttributes(),
				allAttributesOnBothSideOfFDs);
		
		// 2. Find attributes that are only on the right side
		Set<Character> attributesOnlyOnRhsOfFDs = SetUtil.difference(attributesOnRhsSideOfFDs,
				attributesOnLhsSideOfFDs);
		
		// 3. Find attributes that are only on the left side
		Set<Character> attributesOnlyOnLhsOfFDs = SetUtil.difference(attributesOnLhsSideOfFDs,
				attributesOnRhsSideOfFDs);
		
		// 4. find attributes that are on both side
		Set<Character> attributesFoundOnBothSideOfFDs = SetUtil.intersection(attributesOnLhsSideOfFDs,
				attributesOnRhsSideOfFDs);
		
		// 5. Combine the attributes on step 1 and 3
		Set<Character> attributesNotOnLhsOrRhsAndOnlyOnLhsOfFDs = SetUtil.union(attributesNotOnLhsOrRhsOfFDs,
				attributesOnlyOnLhsOfFDs);
		
		Map<Set<Character>, Set<Character>> keys = new HashMap<>();

		// Test if the closures of attributes on step 5 are all the attributes
		Set<Character> closureSet = findClosure(table.getConvertedNonTrivialFDs(),
				attributesNotOnLhsOrRhsAndOnlyOnLhsOfFDs);
		
		if (SetUtil.isMatched(table.getAttributes(), closureSet)) {
			// if yes declare attribute set in 5 as key
			keys.put(attributesNotOnLhsOrRhsAndOnlyOnLhsOfFDs, closureSet);
			table.addKey(attributesNotOnLhsOrRhsAndOnlyOnLhsOfFDs);
		} else {

			// if no
			Combination combination = new Combination(attributesFoundOnBothSideOfFDs);
			for (int i = 1; i <= attributesFoundOnBothSideOfFDs.size(); i++) {
				Set<Set<Character>> setofAttributes = combination.gitCombinationOfLen(i);
				for (Set<Character> characters : setofAttributes) {
					Set<Character> newAttributesSets = new TreeSet<Character>(attributesNotOnLhsOrRhsAndOnlyOnLhsOfFDs);
					newAttributesSets.addAll(characters);
					// only look when new attribute set is not a supper set of
					// already discovered key
					if (!DBUtil.isNewAttributesSetInSuperSetOfAKey(newAttributesSets, keys)) {
						closureSet = findClosure(table.getConvertedNonTrivialFDs(), newAttributesSets);
						if (SetUtil.isMatched(table.getAttributes(), closureSet)) {
							keys.put(newAttributesSets, closureSet);
							table.addKey(newAttributesSets);
						}
					}

				}
			}
		}
		
		return table.getKeys();

	}

	/**
	 * checks where a functional dependency is in BCNF form
	 * 
	 * @param fd the functional dependencies of a relation
	 * @param keys the keys of the relation
	 * @return
	 */
	private static boolean isBCNF(FD fd, Set<Set<Character>> keys) {
		// A relation in in BCNF if for every non-trivial FD X → A, X is a
		// superkey
		Set<Character> lhsOfFD = fd.getAttributesOfLhsOfFD();
		for (Set<Character> key : keys) {
			// check LHS of fd is superkey:constains
			if (SetUtil.isSuperSet(lhsOfFD, key)) {
				return true;
			}
		}

		return false;

	}

	/**
	 * checks whether a functional dependency is in 3NF
	 * 
	 * @param fd functional dependencies
	 * @param keys the keys of the relation
	 * @return
	 */
	private static boolean is3NF(FD fd, Set<Set<Character>> keys) {
		// A relation is in 3NF if for every non-trivial FD X → A, X is a
		// superkey or A is part of some key for R
		Set<Character> lhsOfFD = fd.getAttributesOfLhsOfFD();
		Set<Character> rhsOfFD = fd.getAttributesOfRhsOfFD();
		for (Set<Character> key : keys) {
			// check LHS of fd is superkey:constains
			if (SetUtil.isSuperSet(lhsOfFD, key)) {
				return true;
			}
			if (SetUtil.isSubset(key, rhsOfFD)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * checks whether a functional dependency is in 2NF
	 * 
	 * @param fd functional dependencies
	 * @param keys the keys of the relation
	 * @return
	 */
	private static boolean is2NF(FD fd, Set<Set<Character>> keys) {
		// A relation is in 3NF if for every non-trivial FD X → A, X is a
		Set<Character> lhsOfFD = fd.getAttributesOfLhsOfFD();
		if (DBUtil.isPartOfKeys(lhsOfFD, keys)) {
			return false;
		}
		return true;
	}

	/**
	 * checks  whether a set of attributes is the supper set of any key
	 * 
	 * @param newAttributesSets
	 * @param keys
	 * @return
	 */
	public static boolean isNewAttributesSetInSuperSetOfAKey(Set<Character> newAttributesSets,
			Map<Set<Character>, Set<Character>> keys) {
		for (Map.Entry<Set<Character>, Set<Character>> entry : keys.entrySet()) {
			if (SetUtil.isSuperSet(newAttributesSets, entry.getKey())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * checks whether a set of attributes is part of key
	 * 
	 * @param attributesSets
	 * @param keys
	 * @return
	 */
	public static boolean isPartOfKeys(Set<Character> attributesSets, Set<Set<Character>> keys) {
		for (Set<Character> entry : keys) {
			// check if key match attributes list then it is not part of key
			if (entry.equals(attributesSets)) {
				return false;
			}
			// Now check any of the attribute contains in key
			for (Character ch : attributesSets) {
				if (entry.contains(ch)) {
					return true;
				}
			}
		}
		return false;
	}

}
