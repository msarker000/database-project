package edu.ccny.db.project;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Tuple implements Comparable<Tuple> {

	// store value of key
	private String keyValue;
	private StringBuilder builder = new StringBuilder();
	private Map<Character, Column> values = new LinkedHashMap<Character, Column>();


	public void addColumn(Column col) {
		values.put(col.getName(), col);
	}
	
	public void dropColumn(Column col){
	   values.remove(col.getName());	
	}

	public void setKeyValue(String key) {
		this.keyValue = key;
	}

	public String getKeyValue() {
		return keyValue;
	}

	public String getValue(Set<Character> cols) {
		StringBuilder sb = new StringBuilder();
		for (Character ch : cols) {
			sb.append(values.get(ch).getValue());
		}

		return sb.toString();
	}

	public String getValue(Column column) {
		return values.get(column.getName()).getValue();
	}

	public Map<Character, Column> getValues() {
		return values;
	}

	public void setValue(Character ch, String value) {
		Column col = values.get(ch);
		col.setValue(value);
	}

	public String getValue(Character ch) {
		return values.get(ch).getValue();
	}

	public Column getColumn(Character ch) {
		return values.get(ch);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keyValue == null) ? 0 : keyValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (keyValue == null) {
			if (other.keyValue != null)
				return false;
		} else if (!keyValue.equals(other.keyValue))
			return false;
		return true;
	}

	@Override
	public int compareTo(Tuple other) {
		return this.keyValue.compareTo(other.keyValue);
	}

	public String getHeaderString() {
		builder.setLength(0);
		for (Character key : values.keySet()) {
			builder.append(key + "\t");
		}
		return builder.toString();
	}

	public String getStringValues() {
		builder.setLength(0);
		for (Column column : values.values()) {
			builder.append(column.getValue() + "\t");
		}
		return builder.toString();
	}

	@Override
	public String toString() {
		builder.setLength(0);
		for (Column col : values.values()) {
			builder.append(col.getValue()).append("\t");
		}
		return builder.toString();
	}

}
