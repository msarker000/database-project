package edu.ccny.db.project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Database {

	private Map<String, Table> tables = new HashMap<>();

	public enum LOGICAL {
		AND, OR
	}

	public void addTable(Table table) {
		this.tables.put(table.getName(), table);
	}

	public List<Tuple> select(Table table, List<Condition> conditions, LOGICAL logicType) {
		List<Tuple> tuples = new ArrayList<Tuple>();
		if (logicType == LOGICAL.AND) {
			tuples = table.getTuples().values().stream().filter(tuple -> isSatisfyConditionAll(tuple, conditions))
					.collect(Collectors.toList());
		} else if (logicType == LOGICAL.OR) {
			tuples = table.getTuples().values().stream().filter(tuple -> isSatisfyConditionAny(tuple, conditions))
					.collect(Collectors.toList());
		}

		return tuples;

	}

	private String getGroupingByKey(Tuple tuple, Set<Character> ch) {
		return tuple.getValue(ch);
	}

	public Map<String, List<Tuple>> groupBy(Table table, Set<Character> groupBy) {
		Map<String, List<Tuple>> map = table.getTuples().values().stream()
				.collect(Collectors.groupingBy(p -> getGroupingByKey(p, groupBy), Collectors.toList()));
		return map;
	}

	public void crossJoin(Table table1, Table table2) {

	}

	public void naturalJoin(Table table1, Table table2) {

	}

	public void join(Table table1, Table table2) {

	}

	public void union(Table table1, Table table2) {

	}

	public void intersection(Table table1, Table table2) {

	}

	public void difference(Table table1, Table table2) {

	}

	private boolean isSatisfyConditionAll(Tuple tuple, List<Condition> conditions) {
		for (Condition condition : conditions) {
			Character ch = condition.getAttribute();
			Column column = tuple.getColumn(ch);
			if (!isSatisfyColumnValue(condition, column.getType(), column.getValue())) {
				return false;
			}
		}
		return true;
	}

	private boolean isSatisfyConditionAny(Tuple tuple, List<Condition> conditions) {
		for (Condition condition : conditions) {
			Character ch = condition.getAttribute();
			Column column = tuple.getColumn(ch);
			if (isSatisfyColumnValue(condition, column.getType(), column.getValue())) {
				return true;
			}
		}

		return false;
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
