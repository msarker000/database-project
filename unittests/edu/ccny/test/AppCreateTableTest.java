package edu.ccny.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.ccny.db.project.DBOperator;
import edu.ccny.db.project.DBService;
import edu.ccny.db.project.InputProcessor;
import edu.ccny.db.project.Table;

public class AppCreateTableTest {

	@Test
	public void test() {

		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);

		//student table
		// 'N', "string" - name
		// 'A', "integer" - age
		// 'R', "integer" - roll
		// 'C', "integer" - department Code
		// with primary
		// inputProcdessor.createTable("CREATE TABLE Student( N string NOT NULL,
		// A integer NOT NULL, R integer PRIMARY KEY, C integer NOT NULL);");

	   // 'N', "string" - name of department
		// 'C', "integer" - code of the department
		
		inputProcessor.createTable(
				"CREATE TABLE Department(N string NOT NULL, C integer PRIMARY KEY);");

		// without primary key
		inputProcessor.createTable(
				"CREATE TABLE Student( N string NOT NULL, A integer NOT NULL, R integer  NOT NULL, C integer NOT NULL);");

		Table studentTable = dbService.getTable("Student");
		studentTable.insert("ayub", "35", "100", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");
		studentTable.insert("Yakub", "32", "150", "1003");
		studentTable.insert("Tom", "32", "120", "1004");
		studentTable.insert("Tom2", "32", "130", "1004");
		assertEquals(5, studentTable.getTuples().values().size());
		// ALTER TABLE Persons ADD CONSTRAINT PK_Person PRIMARY KEY (ID,LastName);
		inputProcessor.alterTable("ALTER TABLE Student ADD CONSTRAINT PK_Student PRIMARY KEY(R);");
		
	//	ALTER TABLE Orders
	//	ADD CONSTRAINT FK_PersonOrder
	//	FOREIGN KEY (PersonID) REFERENCES Persons(PersonID);

		inputProcessor.alterTable("ALTER TABLE Student ADD CONSTRAINT FK_Student FOREIGN KEY (C) REFERENCES Department(C);");
	}

}
