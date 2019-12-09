package edu.ccny.db.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;

public class App {

	private DBService dbService = new DBOperator();
	private InputProcessor inputProcessor = new InputProcessor(dbService);

	public void processCommand(String cmd) {
		try {
			if (cmd == null || cmd.isEmpty()) {
				System.err.println("Command string is empty. Please specifiy your command");
				return;
			}

			if (!cmd.contains(";")) {
				System.err.println("Command string should end with semicolone(;)");
				return;
			}

			cmd = cmd.substring(0, cmd.indexOf(';')).replaceAll("\\s+", " ").trim();

			if (cmd.startsWith("CREATE TABLE")) {
				/**
					CREATE TABLE Student( N string NOT NULL, A integer NOT NULL, R
					integer PRIMARY KEY, C integer NOT NULL);
				 */
				
				inputProcessor.createTable(cmd);
			}
			else if (cmd.startsWith("ALTER TABLE")) {
				/**
				  1. ALTER TABLE Student ADD CONSTRAINT PK_Student PRIMARY KEY(R);
				  2. ALTER TABLE Student ADD CONSTRAINT FK_Student FOREIGN KEY (C) REFERENCES Department(C) on DELETE CASCADE;
				  3. ALTER TABLE Student ADD CONSTRAINT check_id_non_zero CHECK (R > 0);
				  4. ALTER TABLE Student DROP CONSTRAINT check_id_non_zero;
				  5. ALTER TABLE Student ADD COLUMN D INTEGER NOT NULL;
				  6. ALTER TABLE Student DROP COLUMN D;
				  7. Alter TABLE Students ADD FD (AB->C, AB->D, C->A, D->B,  A->>B);
				  8. Alter TABLE Students DROP FD (C->A, D->B);
				  */
				inputProcessor.alterTable(cmd);
			}else if (cmd.startsWith("INSERT INTO")) {
				//insert into table
				inputProcessor.insertIntoTable(cmd);
			}else if (cmd.startsWith("DROP TABLE")) {
				//DROP TABLE Students
				inputProcessor.dropTable(cmd);
			}
			else if (cmd.startsWith("SHOW TABLES")) {
				//SHOW TABLES
				inputProcessor.showTables(cmd);
			}
			else if(cmd.startsWith("DELETE")){
				/*
				 1. DELETE FROM students
				 2. DELETE FROM students WHERE A >= 30 AND A <= 6
				 */
				inputProcessor.deleteFromTable(cmd);
			}else if(cmd.startsWith("(SELECT") && (cmd.toUpperCase().contains("UNION")|| 
					cmd.toUpperCase().contains("INTERSECTION") ||cmd.toUpperCase().contains("DIFFERENCE"))){
				/*
				 1. (SELECT * FROM students WHERE A <= 40)  UNION (SELECT * FROM students WHERE C == 1001);
				 2. (SELECT * FROM students WHERE A <= 40)  INTERSECTION (SELECT * FROM students WHERE C == 1001);
				 3. (SELECT * FROM students WHERE A <= 40)  DIFFERENCE (SELECT * FROM students WHERE C == 1001);
				 */
				inputProcessor.executeSetOperation(cmd);
			}
			else if(cmd.startsWith("SELECT *") && (cmd.toUpperCase().contains("NATURALJOIN")|| 
					cmd.toUpperCase().contains("CROSSJOIN") ||cmd.toUpperCase().contains("JOIN")) ){
				/*
				 1. SELECT * FROM table1 NATURALJOIN table2
				 2. SELECT * FROM table1 CROSSJOIN table2
				 3. SELECT * FROM table1 JOIN table2 ON table1.A = table2.C
				 */
			    inputProcessor.executeJoinStatements(cmd);
			}
			else if (cmd.startsWith("SELECT *")) {
				/**
				  1. SELECT * FROM students WHERE N='Ayub';
				  2. SELECT * FROM students GROUPBY(C,A);
				  3. SELECT * FROM students GROUPBY(C);
				  4. SELECT * FROM students WHERE A >= 30 AND A <= 60"; 
				  5. SELECT * FROM students WHERE A >= 30 or A <= 60"; 
				  6. SELECT * FROM students WHERE A >= 30 AND A <= 60" GROUPBY(A, C);
				  7. SELECT * FROM students WHERE A >= 30 or A <= 60" GROUPBY(A, C);
				 */
				inputProcessor.executeSelectStatement(cmd);
				
			}else if(cmd.toUpperCase().startsWith("DESCRIBE TABLE")){
				inputProcessor.describeTable(cmd);
			}else if(cmd.toUpperCase().startsWith("FIND NF FOR")){
				inputProcessor.findNormalFromString(cmd);
			}else if(cmd.toUpperCase().startsWith("FIND KEY FOR")){
				inputProcessor.findFindKeysForTable(cmd);
			}
			else{
				System.err.println("Failed to process your SQL. Please check your command string");	
			}
				
		} catch (Exception ex) {
			System.err.println(String.format("Failed to process your command string. error: %s", ex.getMessage()));
			return;
		}
	}
	
	private static String getUserInput(BufferedReader reader) throws IOException {
		String userInput = "";
		do {
			userInput = reader.readLine();
			if (!userInput.isEmpty() && userInput.equalsIgnoreCase("quit")) {
				System.out.println("You entered QUIT to exit the application");
				System.exit(0);
			}
		} while (userInput.isEmpty());

		return userInput;
	}

	
	public static void main(String[] args) throws IOException {
		System.out.println("****************************************************************");
		System.out.println("********** Database System I Project Console *******************");
		System.out.println("****************************************************************");
		App app = new  App();
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
	
		while(true){
			System.out.print("SQL>> ");
			String cmd = getUserInput(reader);
			app.processCommand(cmd);
		}
		
	}
}
