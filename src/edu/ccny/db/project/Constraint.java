package edu.ccny.db.project;

public class Constraint {

	private final Character attribute;
	private final Operator operator;
	private final String value;
	private final String name;

	public Constraint(Character attribute, Operator operator, String value, String name) {

		this.attribute = attribute;
		this.operator = operator;
		this.value = value;
		this.name = name;
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
		return "name: " + name + "  " + attribute + " " + operator.getName() + " " + value;
	}

	public String getName() {
		return name;
	}
}
