package edu.ccny.db.project;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Tuple {

	// store value of key
	private String keyValue;

	private Map<Character, Column> values = new LinkedHashMap<Character, Column>();

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keyValue == null) ? 0 : keyValue.hashCode());
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}

	public void addColumn(Column col) {
		values.put(col.getName(), col);
	}

	public void setKeyValue(String key) {
		this.keyValue = key;
	}

	public String getKeyValue() {
		return keyValue;
	}
	
	public String getValue(Set<Character> cols){
		StringBuilder sb = new StringBuilder();
		for(Character  ch: cols){
			sb.append(values.get(ch).getValue());
		}
		
		return sb.toString();
	}
	
	public Map<Character, Column> getValues() {
		return values;
	}
	
	public void setValue(Character ch, String value){
		Column col = values.get(ch);
		col.setValue(value);
	}
	
	public String getValue(Character ch){
		return values.get(ch).getValue();
	}
	
	public Column getColumn(Character ch){
		return values.get(ch);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		for(Column col:values.values()){
			sb.append(col.getValue()).append("\t\t");
		}
		return sb.toString();
	}
	
	
}
