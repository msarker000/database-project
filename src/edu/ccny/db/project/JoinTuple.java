package edu.ccny.db.project;

import java.util.LinkedHashMap;
import java.util.Map;

public class JoinTuple {

	private Map<String, Column> values = new LinkedHashMap<String, Column>();
	private StringBuilder builder = new StringBuilder();

	public void add(String key, Column column) {
		values.put(key, column);
	}

	public Map<String, Column> getValues() {
		return values;
	}

	public String getHeaderString() {
		builder.setLength(0);
		for (String key : values.keySet()) {
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
}
