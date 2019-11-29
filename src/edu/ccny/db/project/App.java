package edu.ccny.db.project;

public class App {

	private DBService dbService = new DBOperator();
	private InputProcessor  inputProcessor = new InputProcessor(dbService);
	
	
	public void processCommand(String cmd){
		try{
		if(cmd == null || cmd.isEmpty()){
			System.err.println("Command string is empty. Please specifiy your command");
			return;
		}
		
		if(!cmd.contains(";")){
			System.err.println("Command string should end with semicolone(;)");
			return;
		}
		
		cmd = cmd.substring(0,cmd.indexOf(';')).replaceAll("\\s+", " ").trim();
		
	
		if(cmd.startsWith("CREATE TABLE")){
			inputProcessor.createTable(cmd);
		}
		}catch(Exception ex){
			System.err.println("Failed to process your command string. Please check your command string");
			return;
		}
		
	}
}
