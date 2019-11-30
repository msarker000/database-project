package edu.ccny.db.project;

public class JoinCondition {

	private final Column columnOfFirstTable;

	private final Column columnOfSecondTable;

	public JoinCondition(Column columnOfFirstTable, Column columnOfSecondTable) {
		this.columnOfFirstTable = columnOfFirstTable;
		this.columnOfSecondTable = columnOfSecondTable;
	}

	public Column getColumnOfFirstTable() {
		return columnOfFirstTable;
	}

	public Column getColumnOfSecondTable() {
		return columnOfSecondTable;
	}

}
