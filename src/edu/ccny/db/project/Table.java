package edu.ccny.db.project;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.omg.CORBA.portable.ValueInputStream;

/**
 * This class present table in with all kind if operation insert, update, delete
 * 
 * @author ayub
 *
 */

public class Table {

	private String name;
	private Set<Character> primaryKey;
	private String primaryKeyName;
	private ForeignKey foreignKey;
	public  Map<Character, Column> columns = new LinkedHashMap<Character, Column>();
	private Map<String, Tuple> tuples = new Hashtable<>();
	private StringBuilder primaryKeyValuebuilder = new StringBuilder();

	private Map<String, Constraint> constrainsMap = new LinkedHashMap<>();
	private Map<Character, List<Constraint>> constraints = new LinkedHashMap<Character, List<Constraint>>();
	private List<Table> tablesDependOnMe = new ArrayList<>();

	public Table(String name) {
		this.name = name;
	}

	public void addColumn(Character colName, String type) {
		if (columns.containsKey(colName)) {
			System.err.println(
					String.format("Already contains this Column name in table [%s]. Please check your values", name));
			return;
		}

		Datatype type1 = Datatype.getValueOf(type);
		Column colObj = new Column(colName, type1);
		columns.put(colName, colObj);
		
		for(Tuple tupe:tuples.values()){
			tupe.addColumn(colObj);
		}
	}

	public void addForeignKey(ForeignKey foreignKey) {
		this.foreignKey = foreignKey;
		this.foreignKey.getTable().addTableDependOnMe(this);
	}

	public ForeignKey getForeignKey() {
		return foreignKey;
	}

	public void addTableDependOnMe(Table table) {
		this.tablesDependOnMe.add(table);
	}

	public String getPrimaryKeyName() {
		return primaryKeyName;
	}

	public void addPrimaryKey(Set<Character> primaryKey, String primaryKeyName) {
		this.primaryKey = primaryKey;
		this.primaryKeyName = primaryKeyName;
	}

	public void removePrimaryKey() {
		primaryKey = null;
		primaryKeyName = null;
	}

	public void removeForeignKey() {
		foreignKey = null;
	}

	public void addConstrain(Constraint constraint) {
		List<Constraint> constraintList = this.constraints.get(constraint.getAttribute());
		if (constraintList == null) {
			constraintList = new ArrayList<>();
		}
		constraintList.add(constraint);
		constrainsMap.put(constraint.getName(), constraint);
		constraints.put(constraint.getAttribute(), constraintList);
	}

	/**
	 * remove a constrains from table
	 * 
	 * @param name
	 */
	public boolean removeConstrains(String name) {
		Constraint constraint = constrainsMap.remove(name);
		if (constraint == null) {
			return false;
		}
		List<Constraint> constraintList = this.constraints.get(constraint.getAttribute());
		if (!constraintList.remove(constraint)) {
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @param charAt
	 * @throws Exception 
	 */
	public void removeColumn(char column) throws Exception {
		
		// remove  from columns
		Column removedColumn = columns.remove(column);
		if(removedColumn == null){
			throw new Exception(String.format("Column[%s] is found ", column));
		}

		// remove all constraints
		List<Constraint> constraintList = constraints.remove(column);
		for(Constraint constraint: constraintList){
			constrainsMap.remove(constraint.getName());
		}
		
		if(foreignKey.getKey().contains(column)){
			System.err.println(String.format("Removing foreign key as foreignkey depends on Column[%s] ", column));
			foreignKey = null;
		}
		
		if(primaryKey.contains(column)){
			System.err.println(String.format("Removing primaryKey as primaryKey depends on Column[%s] ", column));
			primaryKey = null;
			primaryKeyName = null;
		}
		
		// remove from tuples
		for(Tuple tupe:tuples.values()){
			tupe.dropColumn(removedColumn);
		}
	}

	/**
	 * insert values into table
	 * 
	 * @param values,
	 *            the values to be inserted
	 * 
	 */
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
			if (primaryKey == null) {
				primaryKeyValuebuilder.append(values[i]);
			} else {
				if (primaryKey.contains(col)) {
					primaryKeyValuebuilder.append(values[i]);
				}
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
	 * finds tuple that matched the key value
	 * 
	 * @param key
	 * @return
	 */

	public Tuple getTuple(String key) {
		return tuples.get(key);

	}

	/**
	 * finds the tuple that matches the column and value
	 * 
	 * @param column
	 *            the column to be matched
	 * @param value
	 *            the value to be matched
	 * @return
	 */
	public Tuple getTuple(Column column, String value) {

		return tuples.values().stream().filter(tupe -> tupe.getValue(column).equals(value)).findFirst().orElse(null);
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
			int constraintValueInInt = constraint.getValue() != null ? Integer.valueOf(constraint.getValue()) : -1;

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
	
	public boolean isValidColumn(Character ch) {
		return columns.containsKey(ch);
		
	}
	
	
	public void deleteAll(){
	   List<String>  keys= 	tuples.keySet().stream().collect(Collectors.toList());
		for(String keyValue: keys){
			delete(keyValue);
		}
	}
	

	public void delete(String keyValue) {
		// first delete from this table
		Tuple tuple = deleteEntryFromTable(this, keyValue);
		String foreignKeyValue = tuple.getValue(foreignKey.getKey());

		// update dependent table based their DELETE_ACTION
		for (Table eachTable : tablesDependOnMe) {
			switch (eachTable.foreignKey.getAction()) {
			case CASCADE:
				deleteAllEntryFromTableByForeignKey(eachTable, foreignKeyValue);
				break;
			case SET_NULL:
				// set null all the entry dependend this tuple
				setNullToAllEntryFromTableByForeignKey(eachTable, foreignKeyValue);
				break;
			case NO_ACTION:
				// Do nothing
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

		// delete all the tuples key
		tupleKeys.forEach(x -> {
			deleteEntryFromTable(table, x);
		});

	}

	private void setNullToAllEntryFromTableByForeignKey(Table table, String foreignKeyValue) {
		// find all the tuple key that match the foreign keyValue
		Set<String> tupleKeys = table.tuples.values().stream().filter(x -> {
			return x.getValue(table.foreignKey.getKey()).equals(foreignKeyValue);
		}).map(x -> x.getKeyValue()).collect(Collectors.toSet());

		// update tuple with null values in the foreign key column
		for (String tupleKey : tupleKeys) {
			Tuple tuple = table.tuples.get(tupleKey);
			for (Character ch : table.foreignKey.getKey()) {
				tuple.setValue(ch, null);
			}
		}
	}

	private Tuple deleteEntryFromTable(Table table, String keyValue) {
		Tuple tuple = table.tuples.remove(keyValue);
		if (tuple == null) {
			System.err.println(String.format("Failed to delete from %s for key %s", name, keyValue));
		}
		return tuple;
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


	public void update(Character ch, String value) {
		for (Tuple tuple : tuples.values()) {
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

	public String describeTable() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("---------------Table Name:%s-----------------------------\n", name));
		sb.append(String.format("---------------List of Columns--------------------------\n", name));
		for (Column column : columns.values()) {
			sb.append(column.printableFormat() + "\n");
		}
		sb.append("\n");
		sb.append(String.format("Primary key:%s , name: %s\n", primaryKey, primaryKeyName));
		if (foreignKey != null) {
			sb.append(String.format("foreignKey key:%s , name: %s , reference table: %s on Delete: %s\n",
					foreignKey.getKey(), foreignKey.getName(), foreignKey.getTable().getName(),
					foreignKey.getAction()));
		}
		sb.append(String.format("--------------------------List of Constraint-----------------------------\n", name));
		for (Constraint constraint : constrainsMap.values()) {
			sb.append(constraint).append("\n");
		}
		sb.append("\n");
		return sb.toString();
	}



}
