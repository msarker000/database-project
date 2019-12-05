package edu.ccny.test;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ccny.db.project.DBOperator;
import edu.ccny.db.project.DBService;
import edu.ccny.db.project.InputProcessor;
import edu.ccny.db.project.Tuple;

public class inputProcessorSetOperation1Test {

	@Test
	public void testJoin() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// department table
		inputProcessor.createTable("CREATE TABLE Departments(N string NOT NULL, C integer PRIMARY KEY);");

		inputProcessor.alterTable("ALTER TABLE Departments ADD CONSTRAINT PK_Departments PRIMARY KEY(C);");

		// student table
		inputProcessor.createTable(
				"CREATE TABLE Students( N string NOT NULL, A integer NOT NULL, R integer  PRIMARY KEY, C integer NOT NULL);");

		inputProcessor.alterTable("ALTER TABLE Students ADD CONSTRAINT PK_Students PRIMARY KEY(R);");
		inputProcessor.alterTable(
				"ALTER TABLE Students ADD CONSTRAINT FK_Students FOREIGN KEY (C) REFERENCES Departments(C) on DELETE CASCADE;");

		inputProcessor.insertIntoTable("INSERT INTO Departments VALUES('DSE', '1001');");
		inputProcessor.insertIntoTable("INSERT INTO Departments VALUES('CHE', '1002');");
		inputProcessor.insertIntoTable("INSERT INTO Departments VALUES('CHE', '1003');");
		inputProcessor.insertIntoTable("INSERT INTO Departments VALUES('CSE', '1004');");

		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('ayub ali', '35', '100', '1001');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('tamer', '30', '110', '1004');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('mikcle', '32', '105', '1001');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('mike pnce', '32', '106', '1004');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('Jhone Jay', '35', '107', '1001');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('GD', '50', '112', '1001');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('Hoe', '65', '108', '1004');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('Woe', '25', '109', '1001');");
		inputProcessor.insertIntoTable("INSERT INTO Students VALUES('Manny', '66', '111', '1003');");
		dbService.printTable("Students");
		
		System.out.println("=======UNION=====");
		Set<Tuple> tuples = inputProcessor.selectSetQueryTable(
				"( SELECT * FROM students WHERE A <= 40 )  UNION ( SELECT * FROM students WHERE C == 1001 );");
		
		
		System.out.println("=======INETRSECTION=====");
		 tuples = inputProcessor.selectSetQueryTable(
				"( SELECT * FROM students WHERE A <= 40 )  INETRSECTION ( SELECT * FROM students WHERE C == 1001 );");
		
		
		System.out.println("======DIFFERENCE======");
		
		tuples = inputProcessor.selectSetQueryTable(
				"( SELECT * FROM students WHERE A <= 40 )  DIFFERENCE ( SELECT * FROM students WHERE C == 1001 );");
		
		dbService.printTuples(tuples);
		//System.out.println(tuples);

	}
}
