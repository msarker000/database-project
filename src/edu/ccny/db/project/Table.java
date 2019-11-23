package edu.ccny.db.project;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class present table in with all kind if operation insert, update, delete
 * 
 * @author ayub
 *
 */

public class Table {
	
	private String name;
	private Set<Character> primaryKey;
	private ForeignKey foreignKey;
	public Map<Character, Column> columns = new LinkedHashMap<Character, Column>();
	private Map<String, Tuple> tuples = new Hashtable<>();
	private StringBuilder primaryKeyValuebuilder = new StringBuilder();
	private Map<Character, List<Constraint>> constraints = new LinkedHashMap<Character, List<Constraint>>();
	private List<Table> tablesDependOnMe = new ArrayList<>();

	public Table(String name) {
		this.name = name;
	}

	public void addColumn(Character colName, String type) {
		if (columns.containsKey(colName)) {
			System.err
					.println(String.format("Already contains this name in table [%s]. Please check your values", name));
			return;
		}

		Datatype type1 = Datatype.getValueOf(type);
		Column colObj = new Column(colName, type1);
		columns.put(colName, colObj);
	}

	public void addForeignKey(ForeignKey foreignKey) {
		this.foreignKey = foreignKey;
		this.foreignKey.getTable().addTableDependOnMe(this);
	}

	public void addTableDependOnMe(Table table) {
		this.tablesDependOnMe.add(table);
	}

	public void addKey(Set<Character> primaryKey) {
		this.primaryKey = primaryKey;
	}

	public void addConstrain(Constraint constraint) {
		List<Constraint> constraintList = this.constraints.get(constraint.getAttribute());
		if (constraintList == null) {
			constraintList = new ArrayList<>();
		}
		constraintList.add(constraint);

		constraints.put(constraint.getAttribute(), constraintList);
	}

	public void insert(String... values) {
		if (values.length != columns.size()) {
			System.err.println(String.format("Failed to insert into [%s]. Please check your values", name));
			return;
		}

		int i = 0;
		Tuple newTuple = new Tuple();
		primaryKeyValuebuilder.setLength(0);
		for (Character col : columns.keySet()) {
			Column column = getColumn(col);
			column.setValue(values[i]);

			// check values to be inserted against constraints
			if (!isValidColumnValue(column, values[i])) {
				return;
			}
			newTuple.addColumn(column);
			if (primaryKey.contains(col)) {
				primaryKeyValuebuilder.append(values[i]);
			}
			i++;
		}

		newTuple.setKeyValue(primaryKeyValuebuilder.toString());

		// check tuple with same primarykey values already exist
		if (tuples.containsKey(newTuple.getKeyValue())) {
			System.err.println(String.format(
					"Failed to insert into [%s]. Primary key violation. Tuple with same key[%s] value exist", name,
					newTuple.getKeyValue()));
			return;
		}

		// if table has foreign key relation then check this foreign key value
		// contains in foreign table
		if (foreignKey != null) {
			String foreignKeyValue = newTuple.getValue(foreignKey.getKey());
			if (!foreignKey.getTable().isTupleExist(foreignKeyValue)) {
				System.err.println(String.format(
						"Failed to insert into [%s]. Foreignkey violation, Value[%s] does not exit in foreign table[%s]",
						name, foreignKey.getKey(), foreignKey.getTable().name));
				return;
			}
		}

		tuples.put(newTuple.getKeyValue(), newTuple);
	}

	/**
	 * checks columns value against constrain
	 * 
	 * @param column
	 *            the column
	 * @param value
	 *            the value to be inserted
	 * 
	 * @return true/false
	 */
	private boolean isValidColumnValue(Column column, String value) {

		List<Constraint> constraintList = constraints.get(column.getName());

		if (constraintList == null) {
			return true;
		}

		// validate column value against all constrain of a column
		boolean isValidAgainstAllConstraint = true;
		for (Constraint constraint : constraintList) {
			if (!isValidColumnValue(constraint, column.getType(), value)) {
				isValidAgainstAllConstraint = false;
			}
		}

		return isValidAgainstAllConstraint;

	}

	public boolean isValidColumnValue(Constraint constraint, Datatype type, String value) {
		if (type == Datatype.STRING) {
			switch (constraint.getOperator()) {
			case EQUAL:
				if (constraint.getValue().equals(value)) {
					return true;
				}
				break;
			case NOT_EQUAL:
				if (constraint.getValue() != value) {
					return true;
				}
				if (constraint.getValue() != null && !constraint.getValue().equals(value)) {
					return true;
				}
				break;
			default:
				break;
			}

		} else if (type == Datatype.INTEGER) {
			int valueInInt = Integer.valueOf(value);
			int constraintValueInInt = Integer.valueOf(constraint.getValue());

			switch (constraint.getOperator()) {
			case GRREATER_THAN:
				if (valueInInt > constraintValueInInt) {
					return true;
				}
				break;
			case GRREATER_THAN_EQUAL:
				if (valueInInt >= constraintValueInInt) {
					return true;
				}
				break;
			case EQUAL:
				if (valueInInt == constraintValueInInt) {
					return true;
				}
				break;
			case NOT_EQUAL:
				if (valueInInt != constraintValueInInt) {
					return true;
				}
				break;
			case LESS_THAN:
				if (valueInInt < constraintValueInInt) {
					return true;
				}
				break;
			case LESS_THAN_EQUAL:
				if (valueInInt <= constraintValueInInt) {
					return true;
				}
				break;

			default:
				break;
			}
		}
		System.err.println(String.format("Failed to insert into [%s]. Constrain violation[%s]", name, constraint));
		return false;
	}

	public boolean isTupleExist(String keyValue) {
		return tuples.containsKey(keyValue);
	}

	public void delete(String keyValue) {
		// first delete from this table
		deleteEntryFromTable(this, keyValue);
		
		// update dependent table based their DELETE_ACTION
		for(Table eachTable: tablesDependOnMe){
			switch(eachTable.foreignKey.getAction()){
			case  CASCADE:
				deleteAllEntryFromTableByForeignKey(eachTable, keyValue);
				break;
			case SET_NULL:
				// set null all the entry dependend this tuple
				setNullToAllEntryFromTableByForeignKey(eachTable, keyValue);
				break;
			case NO_ACTION:
				//Do nothing
				break;
			
			}
			deleteAllEntryFromTableByForeignKey(eachTable, keyValue);
		}

	}

	private void deleteAllEntryFromTableByForeignKey(Table table, String foreignKeyValue) {
		// find all the tuples on the table that matches the foreign keyValue 
		Set<String> tupleKeys = table.tuples.values().stream().filter(x -> {
			return x.getValue(table.foreignKey.getKey()).equals(foreignKeyValue);
		}).map(x -> x.getKeyValue()).collect(Collectors.toSet());

		//delete all the tuples key
		tupleKeys.forEach(x->{
			deleteEntryFromTable(table, x);
		});
		
	}
	
	private void setNullToAllEntryFromTableByForeignKey(Table table, String foreignKeyValue) {
		// find all the tuple key that match the foreign keyValue
		Set<String> tupleKeys = table.tuples.values().stream().filter(x -> {
			return x.getValue(table.foreignKey.getKey()).equals(foreignKeyValue);
		}).map(x -> x.getKeyValue()).collect(Collectors.toSet());

		// update tuple with null values in the foreign key column
		for(String tupleKey: tupleKeys){
			 Tuple tuple =	table.tuples.get(tupleKey);
			 for(Character ch : table.foreignKey.getKey()){
				 tuple.setValue(ch, null);
			 }
		}
	}

	private void deleteEntryFromTable(Table table, String keyValue) {
		Tuple tuple = table.tuples.remove(keyValue);
		if (tuple == null) {
			System.err.println(String.format("Failed to delete from %s for key %s", name, keyValue));
		}
	}

	public String getName() {
		return name;
	}

	public Column getColumn(Character ch) {
		try {
			return (Column) columns.get(ch).clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Map<String, Tuple> getTuples() {
		return tuples;
	}
	
	public void update(Character ch, String value){
		for(Tuple tuple: tuples.values()){
			tuple.setValue(ch, value);
		}
	}

	public void printTuples() {
		System.out.println("----------------------------------");
		System.out.println("         " + name);
		System.out.println("----------------------------------");
		for (Map.Entry<Character, Column> entry : columns.entrySet()) {
			System.out.print(entry.getKey() + " \t\t");
		}
		System.out.println("\n---------------------------------");
		for (Tuple tuple : tuples.values()) {
			System.out.println(tuple);
		}
	}

	@Override
	public String toString() {
		return "Table [name=" + name + ", primaryKey=" + primaryKey + ", foreignKey=" + foreignKey + ", columns="
				+ columns + ", tuples=" + tuples + ", constraints=" + constraints + "]";
	}

}
