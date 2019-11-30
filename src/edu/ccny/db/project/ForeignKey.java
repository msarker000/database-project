package edu.ccny.db.project;

import java.util.LinkedHashSet;
import java.util.Set;

public class ForeignKey {
	private Table table;
	private Set<Character> key = new LinkedHashSet();
    private String name;
	private DeleteAction action;

	public ForeignKey(Table table, Set<Character> key, String name) {
		this.table = table;
		this.key = key;
		action = DeleteAction.NO_ACTION;
		this.name = name;
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
	
	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return "ForeignKey [table=" + table.getName() + ", key=" + key + ", action=" + action + "]";
	}

}
