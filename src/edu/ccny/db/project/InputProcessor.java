package edu.ccny.db.project;

import java.util.LinkedHashSet;
import java.util.Set;

public class InputProcessor {

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
		String tableName = findTableNameFromCommandStr(createTableInputStr);
		String tableColumnsString = createTableInputStr.substring(createTableInputStr.indexOf('(') + 1,
				createTableInputStr.indexOf(')'));
		Table table = new Table(tableName);
		String[] columnStrs = tableColumnsString.split(",");
		for (String columnStr : columnStrs) {
			String[] colParts = columnStr.trim().toUpperCase().split("\\s+");
			Character colCh = colParts[0].charAt(0);
			String dataType = colParts[1];
			table.addColumn(colCh, dataType);
			if (colParts.length == 4) {
				if (colParts[2].concat(colParts[3]).equals(NOTNULL)) {
					table.addConstrain(new Constraint(colCh, Operator.NOT_EQUAL, null, colParts[0].concat(NOTNULL)));
				}
				if (colParts[2].concat(colParts[3]).equals(PRIMARYKEY)) {
					Set<Character> studentkey = new LinkedHashSet<>();
					studentkey.add(colCh);
					table.addPrimaryKey(studentkey);
				}
			}
		}
		dbService.addTable(table);
	}

	public void alterTable(String alterTablestr) {
		// ALTER TABLE Persons ADD CONSTRAINT PK_Person PRIMARY KEY
		// (ID,LastName);
		//System.out.println(alterTablestr);
		String tableName = findTableNameFromCommandStr(alterTablestr);
		Table table = dbService.getTable(tableName);
		alterTablestr = alterTablestr.toUpperCase();
		if (alterTablestr.contains(ADD_CONSTRAIN)) {
			alterTablestr = alterTablestr.substring(alterTablestr.indexOf(ADD_CONSTRAIN) + CONS_STRING_LENGTH).trim();
			String constrainName = alterTablestr.substring(0, alterTablestr.indexOf(' ')).trim();
			String keyType = alterTablestr.substring(alterTablestr.indexOf(' '), alterTablestr.indexOf('(')).trim();

			if (keyType.equalsIgnoreCase("PRIMARY KEY")) {
				// add primary key
				Set<Character> primaryKey = getKeyAttributes(alterTablestr);
				System.out.println(primaryKey);
				table.addPrimaryKey(primaryKey);

			} else if (keyType.equalsIgnoreCase("FOREIGN KEY")) {
				Set<Character> forKey = getKeyAttributes(alterTablestr);
				String foreignTable = alterTablestr.substring(alterTablestr.lastIndexOf(' ')+1, alterTablestr.lastIndexOf('('));
				ForeignKey foreignKey = new ForeignKey(dbService.getTable(foreignTable), forKey);
				table.addForeignKey(foreignKey);
				
			} else if (keyType.equalsIgnoreCase("CHECK")) {

			}
		}

	}

	private Set<Character> getKeyAttributes(String alterTablestr) {
		Set<Character> primaryKey = new LinkedHashSet<>();
		String keyAttributes = alterTablestr
				.substring(alterTablestr.indexOf('(') + 1, alterTablestr.indexOf(')')).trim();
		String[] attributes = keyAttributes.split(",");
		for (String chStr : attributes) {
			primaryKey.add(chStr.charAt(0));
		}
		return primaryKey;
	}

	private String findTableNameFromCommandStr(String cmdSring) {
		cmdSring = cmdSring.substring(0, cmdSring.indexOf('('));
		String tableName = cmdSring.split("\\s+")[2];
		return tableName;
	}

}
