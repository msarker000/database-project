package edu.ccny.db.project;

import java.util.Arrays;
import java.util.Optional;

public enum Operator {
	
	GRREATER_THAN(">"), GRREATER_THAN_EQUAL(">="), EQUAL("=="), NOT_EQUAL("!="), LESS_THAN("<"), LESS_THAN_EQUAL("<=");
	
	private String name;

	Operator(String value){
		this.name =  value;
	}
	
	public String getName() {
		return name;
	}
	
	 public static Optional<Operator> fromText(String text) {
	        return Arrays.stream(values())
	          .filter(bl -> bl.name.equalsIgnoreCase(text))
	          .findFirst();
	    }
	
	
}
