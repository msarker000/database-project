package edu.ccny.db.project;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import edu.ccny.db.project.DBService.LOGICAL;

public class InputProcessor {

	private static final String ON_DELETE_SET_NULL = "ON DELETE SET NULL";
	private static final String ON_DELETE_CASCADE = "ON DELETE CASCADE";
	private static final String FIND_NF = "FIND NF";
	private static final String FIND_KEY = "FIND KEY";
	private static final String DROP_FD = "DROP FD";
	private static final String ADD_FD = "ADD FD";
	private static final String ON = "ON";
	private static final String FROM = "FROM";
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

	public void dropTable(String dropTableStr) {
		// DROP TABLE table
		if (dropTableStr.toUpperCase().contains("DROP TABLE")) {
			String[] strs = dropTableStr.split(" ");
			String tableName = strs[2];
			dbService.dropTable(tableName);
		}
	}

	public void showTables(String showTablesStr) {
		// SHOW TABLES
		if (showTablesStr.toUpperCase().equals("SHOW TABLES")) {
			dbService.showTableList();
		}
	}

	public void describeTable(String describeStr) {
		// DESCIRBE TABLE Students
		if (describeStr.toUpperCase().contains("DESCRIBE TABLE")) {
			String[] strs = describeStr.split(" ");
			String tableName = strs[2];
			Table table = dbService.getTable(tableName);
			if (table == null) {
				System.err.println(String.format("[%s] not found", tableName));
				return;
			}
			String desStr = table.describeTable();
			System.out.println(desStr);
		}
	}

	public void alterTable(String alterTablestr) throws Exception {
		String tableName = findTableNameFromAlterTableStr(alterTablestr);
		Table table = dbService.getTable(tableName);
		if (table == null) {
			throw new Exception(String.format("[%s] is not found in database.", tableName));
		}
		alterTablestr = alterTablestr.toUpperCase();

		if (alterTablestr.contains(ADD_CONSTRAIN)) {
			// add constraint to table
			addConstraintToTable(alterTablestr, table);
		} else if (alterTablestr.contains(DROP_CONSTRAINT)) {
			// drop constraint from table
			dropConstraintFromTable(alterTablestr, table);
		} else if (alterTablestr.contains(ADD_COLUMN)) {
			// add column to table
			String columnStr = alterTablestr.substring(alterTablestr.indexOf(ADD_COLUMN) + ADD_COLUMN.length()).trim();
			addColumnAndConstraintFromColumnStr(tableName, table, columnStr);
		} else if (alterTablestr.contains(DROP_COLUMN)) {
			// remove column from table
			String columnStr = alterTablestr.substring(alterTablestr.indexOf(DROP_COLUMN) + DROP_COLUMN.length())
					.trim();
			table.removeColumn(columnStr.charAt(0));
		} else if (alterTablestr.contains(ADD_FD)) {
			// add FD to table
			addFDToTable(table, alterTablestr);
		} else if (alterTablestr.contains(DROP_FD)) {
			// drop FD from table
			dropFDFromTable(table, alterTablestr);
		}
	}

	/**
	 * @param findString
	 * @return
	 * @throws Exception
	 */
	public void findFindKeysForTable(String findString) throws Exception {
		Set<String> keys = new LinkedHashSet<>();
		if (findString.toUpperCase().contains(FIND_KEY)) {
			String[] strs = findString.split(" ");
			String tableName = strs[3];
			Table table = dbService.getTable(tableName);
			Set<Set<Character>> tablekeys = DBUtil.findKeys(table);
			for (Set<Character> key : tablekeys) {
				keys.add(SetUtil.setToString(key));
			}
		}
		for (String key : keys) {
			System.out.println("\t " + key);
		}
	}

	public void findNormalFromString(String findString) {
		NormalForm normalForm = findNormalFromForTable(findString);
		System.out.println(normalForm);
	}

	/**
	 * 
	 * @param findString
	 * @return
	 */
	public NormalForm findNormalFromForTable(String findString) {
		NormalForm normalForm = NormalForm.NONE;
		if (findString.toUpperCase().contains(FIND_NF)) {
			String[] strs = findString.split(" ");
			String tableName = strs[3];
			Table table = dbService.getTable(tableName);
			DBUtil.findKeys(table);
			normalForm = DBUtil.findNormalForm(table);
		}
		return normalForm;
	}

	private void addConstraintToTable(String alterTablestr, Table table) throws Exception {
		alterTablestr = alterTablestr.substring(alterTablestr.indexOf(ADD_CONSTRAIN) + CONS_STRING_LENGTH).trim();
		String constrainName = alterTablestr.substring(0, alterTablestr.indexOf(' ')).trim();
		String keyType = alterTablestr.substring(alterTablestr.indexOf(' '), alterTablestr.indexOf('(')).trim();
		if (keyType.equalsIgnoreCase("PRIMARY KEY")) {
			// add primary key
			addPrimaryKey(alterTablestr, table, constrainName);
		} else if (keyType.equalsIgnoreCase("FOREIGN KEY")) {
			// add foreign key
			addForeignKey(alterTablestr, table, constrainName);
		} else if (keyType.equalsIgnoreCase("CHECK")) {
			// add constrain
			addCheckConstraint(alterTablestr, table, constrainName);
		}
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

	private void dropFDFromTable(Table table, String alterTablestr) {
		String fdString = alterTablestr.substring(alterTablestr.indexOf("(") + 1, alterTablestr.indexOf(")")).trim();
		String[] fdStrs = fdString.replace(" ", "").split(",");
		for (String fdStr : fdStrs) {
			boolean isValidFd = fdStr.matches("[A-Z]+(->|->>)[A-Z]+");
			if (!isValidFd) {
				System.err.println(String.format("\nWrong format:Enter FD in X->Y format. InValid FD[%s]", fdStr));
			} else {
				boolean isMVD = false;
				String[] fdParts = null;
				if (fdStr.contains("->>")) {
					fdParts = fdStr.split("->>");
					isMVD = true;
				} else {
					fdParts = fdStr.split("->");
				}
				Set<Character> lhsOfFD = SetUtil.getSetFromString(fdParts[0]);
				Set<Character> rhsOfFD = SetUtil.getSetFromString(fdParts[1]);
				if (isMVD) {
					MVD fd = new MVD(lhsOfFD, rhsOfFD);
					table.dropMVD(fd);
				} else {
					FD fd = new FD(lhsOfFD, rhsOfFD);
					table.dropFD(fd);
				}

			}
		}
	}

	private void addFDToTable(Table table, String alterTablestr) {
		String fdString = alterTablestr.substring(alterTablestr.indexOf("(") + 1, alterTablestr.indexOf(")")).trim();
		String[] fdStrs = fdString.replace(" ", "").split(",");
		for (String fdStr : fdStrs) {
			boolean isValidFd = fdStr.matches("[A-Z]+(->|->>)[A-Z]+");
			if (!isValidFd) {
				System.err.println(String.format("\nWrong format:Enter FD in X->Y format. InValid FD[%s]", fdStr));
			} else {
				boolean isMVD = false;
				String[] fdParts = null;
				if (fdStr.contains("->>")) {
					fdParts = fdStr.split("->>");
					isMVD = true;
				} else {
					fdParts = fdStr.split("->");
				}
				Set<Character> lhsOfFD = SetUtil.getSetFromString(fdParts[0]);
				Set<Character> rhsOfFD = SetUtil.getSetFromString(fdParts[1]);
				// IF FD not in the standard
				// non-trivial forms are changed to non-trivial ones,
				// e.g., AB->CD
				// is translated into standard non-trivial forms
				// AB->C
				// and AB->D
				if (isValidFD(table, lhsOfFD, rhsOfFD)) {
					if (isMVD) {
						table.addMVD(lhsOfFD, rhsOfFD);
					} else {
						table.addFD(lhsOfFD, rhsOfFD);
					}

				}
			}
		}
	}

	private void dropConstraintFromTable(String alterTablestr, Table table) throws Exception {
		String constaintName = alterTablestr
				.substring(alterTablestr.indexOf(DROP_CONSTRAINT) + DROP_CONSTRAINT.length() + 1).trim();
		// first try to remove from constrains
		boolean isRemoved = table.removeConstrains(constaintName);
		// if it is not in constraint list of table, check if it is forign
		// key constraint
		if (!isRemoved) {
			ForeignKey foreignKey = table.getForeignKey();
			if (foreignKey != null && foreignKey.getName().equalsIgnoreCase(constaintName)) {
				table.removeForeignKey();
			} else if (table.getPrimaryKeyName().equalsIgnoreCase(constaintName)) {
				table.removePrimaryKey();
			} else {
				throw new Exception("Failed to remove constrains.");
			}

		}
	}

	private void addCheckConstraint(String alterTablestr, Table table, String constrainName) throws Exception {
		String checkString = alterTablestr.substring(alterTablestr.indexOf('(') + 1, alterTablestr.lastIndexOf(')'))
				.trim();
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

	private void addForeignKey(String alterTablestr, Table table, String constrainName) throws Exception {
		Set<Character> forKey = getKeyAttributes(alterTablestr);
		// check whether all the attring is in table
		isValidColumnAttributes(table, forKey);

		String foreignTable = alterTablestr
				.substring(alterTablestr.indexOf(REFERENCES) + REFERENCES.length(), alterTablestr.lastIndexOf('('))
				.trim();
		ForeignKey foreignKey = new ForeignKey(dbService.getTable(foreignTable), forKey, constrainName);
		if (alterTablestr.contains(ON_DELETE_CASCADE)) {
			foreignKey.setAction(DeleteAction.CASCADE);

		} else if (alterTablestr.contains(ON_DELETE_SET_NULL)) {
			foreignKey.setAction(DeleteAction.SET_NULL);
		} else {
			foreignKey.setAction(DeleteAction.NO_ACTION);
		}
		table.addForeignKey(foreignKey);
	}

	private void addPrimaryKey(String alterTablestr, Table table, String constrainName) throws Exception {
		Set<Character> primaryKey = getKeyAttributes(alterTablestr);
		isValidColumnAttributes(table, primaryKey);
		table.addPrimaryKey(primaryKey, constrainName);
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
			throw new Exception(String.format("[%s] is not found in database.", tableName));
		}

		if (!insertString.toUpperCase().contains("VALUES")) {
			throw new Exception("Invalid Insert String.");
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

	public void executeSelectStatement(String slectStr) {
		List<Tuple> tuples = selectFromTable(slectStr);
		dbService.printTuples(tuples);
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

		/// DELETE FROM students
		if (deleteStr.toUpperCase().contains("DELETE") && !deleteStr.toUpperCase().contains(WHERE)) {
			String tableName = findTableNameFromDeleteStr(deleteStr);
			dbService.delete(tableName);
		}
		// DELETE FROM students WHERE A >= 30 AND A <= 60"
		else if (deleteStr.toUpperCase().contains("DELETE") && deleteStr.toUpperCase().contains(WHERE)) {
			String tableName = findTableNameFromDeleteStr(deleteStr);
			String conditionalStr = deleteStr.substring(deleteStr.indexOf(WHERE) + WHERE.length() + 1);
			List<Condition> conditions = getConditionsFromInputString(conditionalStr);
			LOGICAL logicType = LOGICAL.OR;
			if (conditionalStr.contains("AND") || conditionalStr.contains("and")) {
				logicType = LOGICAL.AND;
			}
			dbService.delete(tableName, conditions, logicType);
		}

	}

	public void executeSetOperation(String setOpcmd) {
		Set<Tuple> tuples = selectSetQueryTable(setOpcmd);
		dbService.printTuples(tuples);
	}

	// ( SELECT * FROM students WHERE A <= 40 ) UNION ( SELECT * FROM students
	// WHERE C == 1001 )
	public Set<Tuple> selectSetQueryTable(String setOptString) {
		String firstPart = setOptString.substring(setOptString.indexOf("(") + 1, setOptString.indexOf(")")).trim();
		String secondPart = setOptString.substring(setOptString.lastIndexOf("(") + 1, setOptString.lastIndexOf(")"))
				.trim();

		Set<Tuple> tuples1 = selectFromTable(firstPart).stream().collect(Collectors.toSet());
		Set<Tuple> tuples2 = selectFromTable(secondPart).stream().collect(Collectors.toSet());

		if (setOptString.toUpperCase().contains("UNION")) {
			return dbService.union(tuples1, tuples2);
		} else if (setOptString.toUpperCase().contains("INTERSECTION")) {
			return dbService.intersection(tuples1, tuples2);
		} else if (setOptString.toUpperCase().contains("DIFFERENCE")) {
			return dbService.difference(tuples1, tuples2);
		}

		return new HashSet<>();
	}

	public void executeJoinStatements(String joinQuery) {
		Set<JoinTuple> joinTuples = selectJoinTable(joinQuery);
		dbService.printJoinTuples(joinTuples);
	}

	public Set<JoinTuple> selectJoinTable(String joinQuery) {

		if (joinQuery.toUpperCase().contains("NATURALJOIN")) {
			/// SELECT * FROM table1 NATURALJOIN table2
			String[] strs = joinQuery.trim().split(" ");
			String tableName1 = strs[3];
			String tableName2 = strs[5];

			return dbService.naturalJoin(tableName1, tableName2);

		} else if (joinQuery.toUpperCase().contains("CROSSJOIN")) {

			/// SELECT * FROM table1 CROSSJOIN table2
			String[] strs = joinQuery.trim().split(" ");
			String tableName1 = strs[3];
			String tableName2 = strs[5];
			return dbService.crossJoin(tableName1, tableName2);
		} else if (joinQuery.toUpperCase().contains("JOIN")) {
			// SELECT * FROM table1 JOIN table2 ON table1.A = table2.C
			String[] strs = joinQuery.trim().split(" ");
			String tableName1 = strs[3];
			String tableName2 = strs[5];

			Table table1 = dbService.getTable(tableName1);
			Table table2 = dbService.getTable(tableName2);
			char firstCond = strs[7].charAt(strs[7].length() - 1);
			char secondCond = strs[9].charAt(strs[9].length() - 1);

			JoinCondition joinCondition = new JoinCondition(table1.getColumn(firstCond), table2.getColumn(secondCond));
			return dbService.join(tableName1, tableName2, joinCondition);

		}

		return new HashSet<>();
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

	private boolean isValidFD(Table table, Set<Character> lhsOfFD, Set<Character> rhsOfFD) {

		if (SetUtil.isSubset(lhsOfFD, rhsOfFD)) {
			System.err.println(String.format("\nWrong FD[RHS[%s] of FD is subset of LHS[%s]].", getString(lhsOfFD), getString(rhsOfFD)));
			return false;
		}

		if (!table.getAttributes().containsAll(lhsOfFD)) {
			System.err.println(String.format("\n Wrong FD[LHS[%s] contains attributes not in Relation].",getString(lhsOfFD)));
			return false;

		}

		if (!table.getAttributes().containsAll(rhsOfFD)) {
			System.err.println(String.format("\n Wrong FD[RHS[%s] contains attributes not in Relation].", getString(rhsOfFD)));
			return false;
		}

		if (table.isExistFD(lhsOfFD, rhsOfFD)) {
			System.err.println("\n  Wrong FD[Redundant FD. Same FD exist in Relation]"+ getString(lhsOfFD) +" -> "+ getString(lhsOfFD));
			return false;
		}

		return true;
	}
	
	public String getString(Set<Character> characters){
		return characters.stream().map(c -> String.valueOf(c)).collect(Collectors.joining());
	}

}
