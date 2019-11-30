package edu.ccny.db.project;

enum Datatype {
	STRING, INTEGER, NOT_SUPORTED;

	public static Datatype getValueOf(String type) {
		if (type.equalsIgnoreCase("string")) {
			return STRING;
		} else if (type.equalsIgnoreCase("integer")) {
			return INTEGER;

		} else {
			return NOT_SUPORTED;
		}
	}
}
