package edu.ccny.db.project;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import edu.ccny.db.project.DBService.LOGICAL;

public class InputProcessor {

	private static final String GROUPBY = "GROUPBY";
	private static final String WHERE = "WHERE";
	private static final String DROP_COLUMN = "DROP COLUMN";
	private static final String ADD_COLUMN = "ADD COLUMN";
	private static final String DROP_CONSTRAINT = "DROP CONSTRAINT";
	private static final String REFERENCES = "REFERENCES";
	private static final String ADD_CONSTRAIN = "ADD CONSTRAIN";
	private static final int CONS_STRING_LENGTH = ADD_CONSTRAIN.length() + 1;
	private static final String PRIMARYKEY = "PRIMARYKEY";
	private static final String NOTNULL = "NOTNULL";
	private DBService dbService;

	public InputProcessor(DBService dbService) {
		this.dbService = dbService;
	}

	public void createTable(String createTableInputStr) {

		// CREATE TABLE Student( N string NOT NULL, A integer NOT NULL, R
		// integer PRIMARY KEY, C integer NOT NULL);
		String tableName = findTableNameFromCreateTableStr(createTableInputStr);
		String tableColumnsString = createTableInputStr.substring(createTableInputStr.indexOf('(') + 1,
				createTableInputStr.indexOf(')'));
		Table table = new Table(tableName);
		String[] columnStrs = tableColumnsString.split(",");
		for (String columnStr : columnStrs) {
			addColumnAndConstraintFromColumnStr(tableName, table, columnStr);
		}
		dbService.addTable(table);
	}

	private void addColumnAndConstraintFromColumnStr(String tableName, Table table, String columnStr) {
		String[] colParts = columnStr.trim().toUpperCase().split("\\s+");
		Character colCh = colParts[0].charAt(0);
		String dataType = colParts[1];
		table.addColumn(colCh, dataType);
		if (colParts.length == 4) {
			if (colParts[2].concat(colParts[3]).equals(NOTNULL)) {
				table.addConstrain(new Constraint(colCh, Operator.NOT_EQUAL, null, colParts[0].concat("_" + NOTNULL)));
			}
			if (colParts[2].concat(colParts[3]).equals(PRIMARYKEY)) {
				Set<Character> primaryKey = new LinkedHashSet<>();
				primaryKey.add(colCh);
				table.addPrimaryKey(primaryKey, "PK_" + tableName);
			}
		}
	}

	public void alterTable(String alterTablestr) throws Exception {
		String tableName = findTableNameFromAlterTableStr(alterTablestr);
		Table table = dbService.getTable(tableName);
		if (table == null) {
			throw new Exception("Table name is not found in database. Please check your query");
		}
		alterTablestr = alterTablestr.toUpperCase();
		if (alterTablestr.contains(ADD_CONSTRAIN)) {
			alterTablestr = alterTablestr.substring(alterTablestr.indexOf(ADD_CONSTRAIN) + CONS_STRING_LENGTH).trim();
			String constrainName = alterTablestr.substring(0, alterTablestr.indexOf(' ')).trim();
			String keyType = alterTablestr.substring(alterTablestr.indexOf(' '), alterTablestr.indexOf('(')).trim();

			if (keyType.equalsIgnoreCase("PRIMARY KEY")) {
				// add primary key
				Set<Character> primaryKey = getKeyAttributes(alterTablestr);
				isValidColumnAttributes(table, primaryKey);
				table.addPrimaryKey(primaryKey, constrainName);
			} else if (keyType.equalsIgnoreCase("FOREIGN KEY")) {
				// add foreign key
				Set<Character> forKey = getKeyAttributes(alterTablestr);
				// check whether all the attring is in table
				isValidColumnAttributes(table, forKey);

				String foreignTable = alterTablestr.substring(alterTablestr.indexOf(REFERENCES) + REFERENCES.length(),
						alterTablestr.lastIndexOf('(')).trim();
				ForeignKey foreignKey = new ForeignKey(dbService.getTable(foreignTable), forKey, constrainName);
				if (alterTablestr.contains("ON DELETE CASCADE")) {
					foreignKey.setAction(DeleteAction.CASCADE);

				} else if (alterTablestr.contains("ON DELETE SET NULL")) {
					foreignKey.setAction(DeleteAction.SET_NULL);
				} else {
					foreignKey.setAction(DeleteAction.NO_ACTION);
				}
				table.addForeignKey(foreignKey);
			} else if (keyType.equalsIgnoreCase("CHECK")) {

				// add constrain
				String checkString = alterTablestr
						.substring(alterTablestr.indexOf('(') + 1, alterTablestr.lastIndexOf(')')).trim();
				String[] checkStrings = checkString.split(" ");
				Character columnCh = checkStrings[0].trim().charAt(0);
				Operator operator = Operator.fromText(checkStrings[1].trim());
				String value = checkStrings[2].trim();
				Column column = table.getColumn(columnCh);
				if (column == null) {
					throw new Exception("Column name is not found in database");
				}
				Constraint constraint = new Constraint(columnCh, operator, value, constrainName);
				table.addConstrain(constraint);
			}
		} else if (alterTablestr.contains(DROP_CONSTRAINT)) {
			String constaintName = alterTablestr
					.substring(alterTablestr.indexOf(DROP_CONSTRAINT) + DROP_CONSTRAINT.length() + 1).trim();
			// first try to remove from constrains
			boolean isRemoved = table.removeConstrains(constaintName);
			// if it is not in constraint list of table, check if it is forign
			// key constraint
			if (!isRemoved) {
				ForeignKey foreignKey = table.getForeignKey();
				if (foreignKey.getName().equalsIgnoreCase(constaintName)) {
					table.removeForeignKey();
				} else if (table.getPrimaryKeyName().equalsIgnoreCase(constaintName)) {
					table.removePrimaryKey();
				} else {
					throw new Exception("Failed to remove constrains. check constraint name");
				}
			}
		} else if (alterTablestr.contains(ADD_COLUMN)) {
			String columnStr = alterTablestr.substring(alterTablestr.indexOf(ADD_COLUMN) + ADD_COLUMN.length()).trim();
			addColumnAndConstraintFromColumnStr(tableName, table, columnStr);

		} else if (alterTablestr.contains(DROP_COLUMN)) {
			String columnStr = alterTablestr.substring(alterTablestr.indexOf(DROP_COLUMN) + DROP_COLUMN.length())
					.trim();
			table.removeColumn(columnStr.charAt(0));
		}
	}

	/**
	 * insert into a table in full
	 * 
	 * @param insertString
	 *            input string cmd
	 * @throws Exception
	 */
	public void insertIntoTable(String insertString) throws Exception {

		String tableName = findTableNameFromInsertTableStr(insertString);
		Table table = dbService.getTable(tableName);
		if (table == null) {
			throw new Exception("Table name is not found in database. Please check your query");
		}

		if (!insertString.toUpperCase().contains("VALUES")) {
			throw new Exception("Invalid Insert String please check your input");
		}

		insertString = insertString.substring(insertString.indexOf("VALUES")).trim();

		String insertColumnValuesString = insertString
				.substring(insertString.indexOf('(') + 1, insertString.indexOf(')')).trim();

		String[] insValues = insertColumnValuesString.split(",");
		String[] valuesToInsert = new String[insValues.length];
		int i = 0;
		for (String value : insValues) {
			value = value.trim();
			value = value.substring(1, value.length() - 1);
			valuesToInsert[i++] = value;
		}
		table.insert(valuesToInsert);
	}

	/**
	 * select from table
	 * 
	 * @param string
	 *            the input string
	 */
	public List<Tuple> selectFromTable(String selcetStr) {

		// SELECT * FROM students WHERE N='Ayub'

		/// SELECT * FROM students
		if (selcetStr.contains("*") && !selcetStr.toUpperCase().contains(WHERE)
				&& !selcetStr.toUpperCase().contains(GROUPBY)) {

			String tableName = findTableNameFromSelectStr(selcetStr);
			return dbService.select(tableName).stream().collect(Collectors.toList());
		}

		// SELECT * FROM students GROUPBY(C,A)
		else if (selcetStr.contains("*") && !selcetStr.toUpperCase().contains(WHERE)
				&& selcetStr.toUpperCase().contains(GROUPBY)) {

			String tableName = findTableNameFromSelectStr(selcetStr);
			Set<Character> groupByChars = getGroupByAttributes(selcetStr);
			return dbService.selectWithGroupBy(tableName, groupByChars);

		}

		// SELECT * FROM students WHERE A >= 30 AND A <= 60" ; supports only AND
		// or only OR
		else if (selcetStr.contains("*") && selcetStr.toUpperCase().contains(WHERE)
				&& !selcetStr.toUpperCase().contains(GROUPBY)) {

			String tableName = findTableNameFromSelectStr(selcetStr);
			String conditionalStr = selcetStr.substring(selcetStr.indexOf(WHERE) + WHERE.length() + 1);

			List<Condition> conditions = getConditionsFromInputString(conditionalStr);

			LOGICAL logicType = LOGICAL.OR;
			if (conditionalStr.contains("AND") || conditionalStr.contains("and")) {
				logicType = LOGICAL.AND;
			}
			return dbService.select(tableName, conditions, logicType).stream().collect(Collectors.toList());

		}

		// SELECT * FROM students WHERE A >= 30 AND A <= 60" GROUPBY(A, C) ;
		// supports only AND
		// or only OR
		else if (selcetStr.contains("*") && selcetStr.toUpperCase().contains(WHERE)
				&& selcetStr.toUpperCase().contains(GROUPBY)) {
			String tableName = findTableNameFromSelectStr(selcetStr);
			String conditionalStr = selcetStr.substring(selcetStr.indexOf(WHERE) + WHERE.length() + 1,
					selcetStr.indexOf(GROUPBY));
			Set<Character> groupByChars = getGroupByAttributes(selcetStr);
			List<Condition> conditions = getConditionsFromInputString(conditionalStr);

			LOGICAL logicType = LOGICAL.OR;
			if (conditionalStr.contains("AND") || conditionalStr.contains("and")) {
				logicType = LOGICAL.AND;
			}

			List<Tuple> tuples = dbService.select(tableName, conditions, logicType).stream()
					.collect(Collectors.toList());
			return dbService.selectWithGroupBy(tuples, groupByChars);
		}
		return new ArrayList<>();
	}
	
	public void deleteFromTable(String deleteStr) {
		// TODO Auto-generated method stub

		/// DELETE  FROM students
		if (deleteStr.toUpperCase().contains("DELETE") && !deleteStr.toUpperCase().contains(WHERE)) {
			String tableName = findTableNameFromDeleteStr(deleteStr);
			dbService.delete(tableName);
		}
		
	}
	
	public List<JoinTuple> selectJoinTable(String joinString) {
			if(joinString.toUpperCase().contains("UNION")){
				//TODO do union
			}else if(joinString.toUpperCase().contains("INTERSECTION")){
				//TODO do INTERSECTION
			}else if(joinString.toUpperCase().contains("DIFFERENCE")){
				//TODO DIFFERENCE
			}
		
		
		return new ArrayList<>();
	}

	private List<Condition> getConditionsFromInputString(String conditionalStr) {
		List<Condition> conditions = new ArrayList<>();
		String[] conditionalStrs = null;
		if (conditionalStr.contains("AND") || conditionalStr.contains("OR")) {
			conditionalStrs = conditionalStr.split("AND|OR");
		} else if (conditionalStr.contains("and") || conditionalStr.contains("or")) {
			conditionalStrs = conditionalStr.split("and|or");
		} else {
			conditionalStrs = conditionalStr.split("AND");
		}

		for (String conditionStr : conditionalStrs) {
			String[] conditionalTokens = conditionStr.trim().split(" ");
			conditions.add(new Condition(conditionalTokens[0].trim().charAt(0),
					Operator.fromText(conditionalTokens[1].trim()), conditionalTokens[2].trim(), ""));
		}
		return conditions;
	}

	private Set<Character> getGroupByAttributes(String selcetStr) {
		Set<Character> groupByChars = new LinkedHashSet<>();
		String groupByStr = selcetStr.substring(selcetStr.indexOf(GROUPBY) + GROUPBY.length()).trim();

		groupByStr = groupByStr.substring(1, groupByStr.length() - 1);
		String[] groupStrs = groupByStr.split(",");
		for (String group : groupStrs) {
			groupByChars.add(group.trim().charAt(0));

		}
		return groupByChars;
	}

	private void isValidColumnAttributes(Table table, Set<Character> forKey) throws Exception {
		for (Character ch : forKey) {
			if (!table.isValidColumn(ch)) {
				throw new Exception(String.format(
						"Foreign/Primary key attributes does not exist in table[%s]. Please check your query.",
						table.getName()));
			}
		}
	}

	private Set<Character> getKeyAttributes(String alterTablestr) {
		Set<Character> primaryKey = new LinkedHashSet<>();
		String keyAttributes = alterTablestr.substring(alterTablestr.indexOf('(') + 1, alterTablestr.indexOf(')'))
				.trim();
		String[] attributes = keyAttributes.split(",");
		for (String chStr : attributes) {
			primaryKey.add(chStr.charAt(0));
		}
		return primaryKey;
	}

	private String findTableNameFromCreateTableStr(String cmdSring) {
		cmdSring = cmdSring.substring(0, cmdSring.indexOf('('));
		String tableName = cmdSring.split("\\s+")[2];
		return tableName;
	}

	private String findTableNameFromAlterTableStr(String cmdSring) {
		String tableName = cmdSring.split("\\s+")[2];
		return tableName;
	}

	private String findTableNameFromInsertTableStr(String insertStr) {
		String tableName = insertStr.split("\\s+")[2];
		return tableName;
	}

	private String findTableNameFromSelectStr(String selcetStr) {
		// SELECT * FROM users WHERE city='new york'
		String tableName = selcetStr.split("\\s+")[3];
		return tableName;
	}
	
	private String findTableNameFromDeleteStr(String selcetStr) {
		// DELEET FROM users
		String tableName = selcetStr.split("\\s+")[2];
		return tableName;
	}

	

}
