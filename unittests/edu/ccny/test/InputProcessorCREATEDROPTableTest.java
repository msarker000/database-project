package edu.ccny.test;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.ccny.db.project.DBOperator;
import edu.ccny.db.project.DBService;
import edu.ccny.db.project.InputProcessor;
import edu.ccny.db.project.Table;

public class InputProcessorCREATEDROPTableTest {
	
	@Test
	public void testCreateDropTable(){
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// department table
		inputProcessor.createTable("CREATE TABLE Departments(N string NOT NULL, C integer PRIMARY KEY);");
		inputProcessor.createTable(
				"CREATE TABLE Students( N string NOT NULL, A integer NOT NULL, R integer  PRIMARY KEY, C integer NOT NULL);");

		dbService.showTableList();
		
		inputProcessor.dropTable("DROP TABLE Students");
		
		
		Table table = dbService.getTable("Students");
		assertEquals(null, table);
		
		inputProcessor.dropTable("DROP TABLE Departments");
		table = dbService.getTable("Departments");
		assertEquals(null, table);
		
	}

}
