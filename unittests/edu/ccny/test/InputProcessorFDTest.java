package edu.ccny.test;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.ccny.db.project.DBOperator;
import edu.ccny.db.project.DBService;
import edu.ccny.db.project.InputProcessor;
import edu.ccny.db.project.NormalForm;
import edu.ccny.db.project.Table;

public class InputProcessorFDTest {

	@Test
	public void testAddFDtest() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (C->D, C->A, B->C);");

		inputProcessor.alterTable("Alter TABLE Students DROP FD (C->D);");

		System.out.println(studentTable.describeTable());

	}

	@Test
	public void testFindKeystest() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (C->D, C->A, B->C);");

		inputProcessor.findFindKeysForTable("FIND KEY FOR Students");

		System.out.println(studentTable.describeTable());

	}

	@Test
	public void testFindKeys2ndNFtest() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (C->D, C->A, B->C);");
		NormalForm nf = inputProcessor.findNormalFromForTable("FIND NF FOR Students");

		assertEquals(NormalForm.SECOND_NF, nf);

	}

	@Test
	public void testFindKeysIstNFtest() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (B->C, D->A);");

		NormalForm nf = inputProcessor.findNormalFromForTable("FIND NF FOR Students");

		assertEquals(NormalForm.FIRST_NF, nf);

	}
	
	@Test
	public void testFindKeys3NFtest() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (ABC->D, D->A);");

		NormalForm nf = inputProcessor.findNormalFromForTable("FIND NF FOR Students");
		
		System.out.println(nf);

		assertEquals(NormalForm.THIRD_NF, nf);

	}
	
	@Test
	public void testFindKeys3NFtest2() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (A->B, BC->D, A->C);");

		NormalForm nf = inputProcessor.findNormalFromForTable("FIND NF FOR Students");
		
		System.out.println(nf);

		assertEquals(NormalForm.SECOND_NF, nf);

	}
	
	@Test
	public void testFindKeys3NFtest3() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (AB->C, AB->D, C->A, D->B);");

		NormalForm nf = inputProcessor.findNormalFromForTable("FIND NF FOR Students");
		
		System.out.println(nf);

		assertEquals(NormalForm.THIRD_NF, nf);

	}
	
	

	@Test
	public void testFindKeys3NFtest4() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (AB->C, AB->D, C->A, D->B,  A->>B);");

		NormalForm nf = inputProcessor.findNormalFromForTable("FIND NF FOR Students");
		

		assertEquals(NormalForm.THIRD_NF, nf);
		
		inputProcessor.alterTable("Alter TABLE Students DROP FD (C->A, D->B);");
		nf = inputProcessor.findNormalFromForTable("FIND NF FOR Students");
		
		System.out.println(nf);
		assertEquals(NormalForm.BCNF, nf);

	}
	
	@Test
	public void testCREATEDROPFDMVDtest() throws Exception {
		DBService dbService = new DBOperator();
		InputProcessor inputProcessor = new InputProcessor(dbService);
		// student table
		inputProcessor.createTable("CREATE TABLE Students(A integer PRIMARY KEY, B string NOT NULL, "
				+ " C string NOT NULL,  D string NOT NULL);");

		Table studentTable = dbService.getTable("Students");

		inputProcessor.alterTable("Alter TABLE Students ADD FD (AB->C, AB->D, C->A, D->B,  A->>B);");
		assertEquals(1,studentTable.getMvds().size());
		
		inputProcessor.alterTable("Alter TABLE Students Drop FD (AB->C);");
		assertEquals(1,studentTable.getMvds().size());
		
		inputProcessor.alterTable("Alter TABLE Students DROP FD (A->>B);");
		assertEquals(0,studentTable.getMvds().size());
		
	}
	
	
}
