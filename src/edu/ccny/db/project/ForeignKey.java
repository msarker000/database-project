package edu.ccny.db.project;

import java.util.LinkedHashSet;
import java.util.Set;

public class ForeignKey {
	
	private Table table;
	private Set<Character> key = new LinkedHashSet();
	
	private DeleteAction action;

	
	public ForeignKey(Table table, Set<Character> key) {
		this.table = table;
		this.key = key;
		 action = DeleteAction.NO_ACTION;
	}
	
	public void setAction(DeleteAction action) {
		this.action = action;
	}
	
	public DeleteAction getAction() {
		return action;
	}
	
    public Table getTable() {
		return table;
	}
	
	public Set<Character> getKey() {
		return key;
	}
	
}
