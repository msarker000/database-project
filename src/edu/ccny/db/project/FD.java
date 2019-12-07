package edu.ccny.db.project;


import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This class is for storing function dependency 
 * 
 * @author ayub
 *
 */

public class FD {
	protected final Set<Character> attributesOfLhsOfFD = new TreeSet<Character>();
	protected final  Set<Character> attributesOfRhsOfFD = new TreeSet<Character>();
	private final Set<Character> allAttributesOfbothSide;
	private NormalForm normalForm = NormalForm.NONE;

	boolean isTaken;

	public FD(String lhsOfFDStr, String rhsOfFDStr) {
		for (Character ch : lhsOfFDStr.toCharArray()) {
			this.attributesOfLhsOfFD.add(ch);
		}

		for (Character ch : rhsOfFDStr.toCharArray()) {
			this.attributesOfRhsOfFD.add(ch);
		}
		allAttributesOfbothSide = new HashSet<Character>(attributesOfLhsOfFD);
		allAttributesOfbothSide.addAll(attributesOfRhsOfFD);
	}

	public FD(Set<Character> lhsOfFD, Character rhsOfFD) {

		attributesOfLhsOfFD.addAll(lhsOfFD);
		attributesOfRhsOfFD.add(rhsOfFD);

		allAttributesOfbothSide = new HashSet<Character>(attributesOfLhsOfFD);
		allAttributesOfbothSide.addAll(attributesOfRhsOfFD);
	}

	public FD(Set<Character> lhsOfFD, Set<Character> rhsOfFD) {

		attributesOfLhsOfFD.addAll(lhsOfFD);
		attributesOfRhsOfFD.addAll(rhsOfFD);

		allAttributesOfbothSide = new HashSet<Character>(attributesOfLhsOfFD);
		allAttributesOfbothSide.addAll(attributesOfRhsOfFD);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((attributesOfLhsOfFD == null) ? 0 : attributesOfLhsOfFD.hashCode());
		result = prime * result + ((attributesOfRhsOfFD == null) ? 0 : attributesOfRhsOfFD.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FD other = (FD) obj;
		if (attributesOfLhsOfFD == null) {
			if (other.attributesOfLhsOfFD != null)
				return false;
		} else if (!attributesOfLhsOfFD.equals(other.attributesOfLhsOfFD))
			return false;
		if (attributesOfRhsOfFD == null) {
			if (other.attributesOfRhsOfFD != null)
				return false;
		} else if (!attributesOfRhsOfFD.equals(other.attributesOfRhsOfFD))
			return false;
		return true;
	}

	public void setNormalForm(NormalForm normalForm) {
		this.normalForm = normalForm;
	}
	
	public NormalForm getNormalForm() {
		return normalForm;
	}
	
	public Set<Character> getAttrOfBothSide() {
		return allAttributesOfbothSide;
	}

	@Override
	public String toString() {
		return "FunctionDependency [attributesOfLhsOfFD=" + attributesOfLhsOfFD + ", attributesOfRhsOfFD="
				+ attributesOfRhsOfFD + ", allAttributesOfbothSide=" + allAttributesOfbothSide + ", normalForm="
				+ normalForm.getName() + "]";
	}
	
	public Set<Character> getAllAttributesOfbothSide() {
		return allAttributesOfbothSide;
	}
	
	public Set<Character> getAttributesOfLhsOfFD() {
		return attributesOfLhsOfFD;
	}
	
	public Set<Character> getAttributesOfRhsOfFD() {
		return attributesOfRhsOfFD;
	}

	public String toPrintableFormat() {
		String lhs = attributesOfLhsOfFD.stream().map(x -> String.valueOf(x)).collect(Collectors.joining());
		String rhs = attributesOfRhsOfFD.stream().map(x -> String.valueOf(x)).collect(Collectors.joining());
		return String.format("%s->%s", lhs, rhs);
	}

}