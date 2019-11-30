package edu.ccny.db.project;

import java.util.LinkedHashSet;
import java.util.Set;

public class InputProcessor {

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
			String[] colParts = columnStr.trim().toUpperCase().split("\\s+");
			Character colCh = colParts[0].charAt(0);
			String dataType = colParts[1];
			table.addColumn(colCh, dataType);
			if (colParts.length == 4) {
				if (colParts[2].concat(colParts[3]).equals(NOTNULL)) {
					table.addConstrain(new Constraint(colCh, Operator.NOT_EQUAL, null, colParts[0].concat("_"+NOTNULL)));
				}
				if (colParts[2].concat(colParts[3]).equals(PRIMARYKEY)) {
					Set<Character> primaryKey = new LinkedHashSet<>();
					primaryKey.add(colCh);
					table.addPrimaryKey(primaryKey, "PK_"+tableName);
				}
			}
		}
		dbService.addTable(table);
	}

	public void alterTable(String alterTablestr) throws Exception {
		// ALTER TABLE Persons ADD CONSTRAINT PK_Person PRIMARY KEY
		// (ID,LastName);
		// System.out.println(alterTablestr);
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
				table.addPrimaryKey(primaryKey, constrainName);
			} else if (keyType.equalsIgnoreCase("FOREIGN KEY")) {
				// add foreign key
				Set<Character> forKey = getKeyAttributes(alterTablestr);
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
		}else if(alterTablestr.contains(DROP_CONSTRAINT)){
			String constaintName = alterTablestr.substring(alterTablestr.indexOf(DROP_CONSTRAINT)+DROP_CONSTRAINT.length()+1).trim();
			// first try to remove from constrains
			boolean isRemoved = table.removeConstrains(constaintName);
			//if it is not in constraint list of table, check if it is forign key constraint
			if(!isRemoved){
				ForeignKey foreignKey = table.getForeignKey();
				if(foreignKey.getName().equalsIgnoreCase(constaintName)){
					table.removeForeignKey();
				}else if(table.getPrimaryKeyName().equalsIgnoreCase(constaintName)){
					table.removePrimaryKey();
				}else{
					throw new Exception("Failed to remove constrains. check constraint name");
				}
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

}
