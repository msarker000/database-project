package edu.ccny.db.project;

import java.util.Set;
import java.util.stream.Collectors;

public class MVD extends FD{

	public MVD(Set<Character> lhsOfFD, Set<Character> rhsOfFD) {
		super(lhsOfFD, rhsOfFD);
		
	}
	
	public String toPrintableFormat() {
		String lhs = attributesOfLhsOfFD.stream().map(x -> String.valueOf(x)).collect(Collectors.joining());
		String rhs = attributesOfRhsOfFD.stream().map(x -> String.valueOf(x)).collect(Collectors.joining());
		return String.format("%s->>%s", lhs, rhs);
	}

}
