package edu.ccny.db.project;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Database {

	private Map<String, Table> tables = new HashMap<>();

	public void addTable(Table table) {
		this.tables.put(table.getName(), table);
	}

	public List<Tuple> select(Table table, List<Condition> conditions) {
		List<Tuple> tuples = table.getTuples().values().stream().filter(tuple -> isSatisfyCondition(tuple, conditions))
				.collect(Collectors.toList());
		return tuples;
	}

	private boolean isSatisfyCondition(Tuple tuple, List<Condition> conditions) {
		for (Condition condition : conditions) {
			Character ch = condition.getAttribute();
			Column column = tuple.getColumn(ch);
			if(!isSatisfyColumnValue(condition, column.getType(), column.getValue())){
				return false;
			}
		}
		
		return true;
	}
	
	private boolean isSatisfyColumnValue(Condition condition, Datatype type, String value) {
		if (type == Datatype.STRING) {
			switch (condition.getOperator()) {
			case EQUAL:
				if (condition.getValue().equals(value)) {
					return true;
				}
				break;
			case NOT_EQUAL:
				if (condition.getValue() != value) {
					return true;
				}
				if (condition.getValue() != null && !condition.getValue().equals(value)) {
					return true;
				}
				break;
			default:
				break;
			}

		} else if (type == Datatype.INTEGER) {
			int valueInInt = Integer.valueOf(value);
			int constraintValueInInt = Integer.valueOf(condition.getValue());

			switch (condition.getOperator()) {
			case GRREATER_THAN:
				if (valueInInt > constraintValueInInt) {
					return true;
				}
				break;
			case GRREATER_THAN_EQUAL:
				if (valueInInt >= constraintValueInInt) {
					return true;
				}
				break;
			case EQUAL:
				if (valueInInt == constraintValueInInt) {
					return true;
				}
				break;
			case NOT_EQUAL:
				if (valueInInt != constraintValueInInt) {
					return true;
				}
				break;
			case LESS_THAN:
				if (valueInInt < constraintValueInInt) {
					return true;
				}
				break;
			case LESS_THAN_EQUAL:
				if (valueInInt <= constraintValueInInt) {
					return true;
				}
				break;

			default:
				break;
			}
		}
		return false;
	}

}
