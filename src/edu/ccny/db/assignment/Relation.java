package edu.ccny.db.assignment;


import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This class hold the relation of a user schema
 * 
 * @author ayub
 *
 */
public class Relation {
	private Set<Character> attributes = new TreeSet<Character>();
	
	//Those FDs not in the standard
	//  non-trivial forms are changed to non-trival ones, e.g., AB->CD 
	//  is translated into standard non-trivial forms AB->C and AB->D. 
	private Set<FunctionDependency> convertedNonTrivialFDs = new LinkedHashSet<FunctionDependency>();
	
	//Original set of valid user entered functional dependencies
	private Set<FunctionDependency> originalFDs = new LinkedHashSet<FunctionDependency>();
	private Set<Set<Character>> keys = new LinkedHashSet<Set<Character>>();
	private Set<Character> keyAttributes = new TreeSet<Character>();
	private Set<Character> nonKeyAttributes = new TreeSet<Character>();
	private NormalForm normalForm = NormalForm.NONE;
	

	public Relation(String attributesStr) {
		char[] chars = attributesStr.toCharArray();
		for (char ch : chars) {
			attributes.add(ch);
			nonKeyAttributes.add(ch);
		}
	}

	public void addFD(String lhsOfFDStr, String rhsOfFDStr) {
		originalFDs.add(new FunctionDependency(lhsOfFDStr, rhsOfFDStr));
	}
		
	public void addFD(Set<Character> lhsOfFD, Set<Character> rhsOfFD) {
		
		originalFDs.add(new FunctionDependency(lhsOfFD, rhsOfFD));
		
		if (rhsOfFD.size() > 1) {
			//it is not in standard non-trivial form, need to convert non-trival form
			for (Character ch : rhsOfFD) {
				convertedNonTrivialFDs.add(new FunctionDependency(lhsOfFD, rhsOfFD));
			}
		} else {
			// it is in standard non-trivial form
			convertedNonTrivialFDs.add(new FunctionDependency(lhsOfFD, rhsOfFD));	
		}
		
	}
	
	public void addKey(Set<Character> key) {
		keys.add(key);
		keyAttributes.addAll(key);
		nonKeyAttributes.removeAll(key);
	}

	public String toPrintableFormat() {
		return this.attributes.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(""));
	}
	
	
	public Set<FunctionDependency> getConvertedNonTrivialFDs() {
		return convertedNonTrivialFDs;
	}
	
	public Set<FunctionDependency> getOriginalFDs() {
		return originalFDs;
	}
	
	public Set<Set<Character>> getKeys() {
		return keys;
	}
	
	public Set<Character> getAttributes() {
		return attributes;
	}
	
	public void setNormalForm(NormalForm normalForm) {
		this.normalForm = normalForm;
	}
	
	public NormalForm getNormalForm() {
		return normalForm;
	}

}