package edu.ccny.db.project;

import java.util.Arrays;
import java.util.Optional;

public enum Operator {

	GRREATER_THAN(">"), GRREATER_THAN_EQUAL(">="), EQUAL("=="), NOT_EQUAL("!="), LESS_THAN("<"), LESS_THAN_EQUAL("<=");

	private String name;

	Operator(String value) {
		this.name = value;
	}

	public String getName() {
		return name;
	}

	public static Operator fromText(String text) {
		Operator operator =  Arrays.stream(values()).filter(bl -> bl.name.equalsIgnoreCase(text)).findFirst().orElse(null);
		if(operator == null || text.equals("=")){
			operator = Operator.EQUAL;
		}
		return operator;
	}

}
