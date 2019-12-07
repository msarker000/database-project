package edu.ccny.db.project;


/**
 * This class is for holding all types of normal 
 * 
 * @author ayub
 *
 */
public enum NormalForm {
	FIRST_NF("1NF", 1), SECOND_NF("2NF",2), THIRD_NF("3NF",3), BCNF("BCNF",4), NONE("Not Identified", 5);

	private String name;
	
	//priority is used to determine relation's normal from from list functional dependencies
	private int priority;

	NormalForm(String value, int priority) {
		this.name = value;
		this.priority = priority;
	}
	
	public int getPriority() {
		return priority;
	}
	
	public String getName() {
		return name;
	}

}