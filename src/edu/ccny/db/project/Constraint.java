package edu.ccny.db.project;

public class Constraint {
	
	private final Character attribute;
	private final Operator operator;
	private final String value;
	public Constraint(Character attribute, Operator operator, String value) {
		
		this.attribute = attribute;
		this.operator = operator;
		this.value = value;
	}
	public Character getAttribute() {
		return attribute;
	}
	public Operator getOperator() {
		return operator;
	}
	public String getValue() {
		return value;
	}
	@Override
	public String toString() {
		return attribute + " " + operator.getName()+ " " +value;
	}
	
	
	
}
