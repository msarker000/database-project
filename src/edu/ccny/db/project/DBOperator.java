package edu.ccny.db.project;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class DBOperator implements DBService {

	private Map<String, Table> tables = new HashMap<>();

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#addTable(edu.ccny.db.project.Table)
	 */
	@Override
	public void addTable(Table table) {
		tables.put(table.getName().toLowerCase(), table);
	}
	
	@Override
	public void dropTable(String tableName) {
		tables.remove(tableName.toLowerCase());
	}

	@Override
	public Table getTable(String tableName) {
		return tables.get(tableName.toLowerCase());
	}
	
	@Override
	public void showTableList() {
		if(tables.isEmpty()){
			System.out.println("No Tables");
			return;
		}
		for(Table table: tables.values()){
			System.out.println(table.getName());
		}
	}

	@Override
	public Set<Tuple> select(String tableName) {
		Set<Tuple> tuples = null;
		try {
			Table table = tables.get(tableName.toLowerCase());
			tuples = table.getTuples().values().stream().collect(Collectors.toSet());
			return tuples;
		} catch (Exception ex) {
			System.err.println(
					String.format("Failed to execute SELECT operation on Table[%s]. Check your params", tableName));
		}

		return tuples;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#select(java.lang.String,
	 * java.util.List, edu.ccny.db.project.DBOperator.LOGICAL)
	 */
	@Override
	public Set<Tuple> select(String tableName, List<Condition> conditions, LOGICAL logicType) {
		Set<Tuple> tuples = null;
		try {
			Table table = tables.get(tableName.toLowerCase());

			if (logicType == LOGICAL.AND) {
				tuples = table.getTuples().values().stream().filter(tuple -> isSatisfyConditionAll(tuple, conditions))
						.collect(Collectors.toSet());
			} else if (logicType == LOGICAL.OR) {
				tuples = table.getTuples().values().stream().filter(tuple -> isSatisfyConditionAny(tuple, conditions))
						.collect(Collectors.toSet());
			}
		} catch (Exception ex) {
			System.err.println(
					String.format("Failed to execute SELECT operation on Table[%s]. Check your params", tableName));
		}

		return tuples;

	}

	private String getGroupingByKey(Tuple tuple, Set<Character> ch) {
		return tuple.getValue(ch);
	}

	public List<Tuple> selectWithGroupBy(List<Tuple> tuples, Set<Character> groupBy) {
		Map<String, List<Tuple>> map = tuples.stream()
				.collect(Collectors.groupingBy(p -> getGroupingByKey(p, groupBy), Collectors.toList()));
		return map.values().stream().flatMap(x -> x.stream()).collect(Collectors.toList());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#groupBy(java.lang.String,
	 * java.util.Set)
	 */
	@Override
	public List<Tuple> selectWithGroupBy(String tableName, Set<Character> groupBy) {
		try {
			Table table = tables.get(tableName.toLowerCase());

			Map<String, List<Tuple>> map = table.getTuples().values().stream()
					.collect(Collectors.groupingBy(p -> getGroupingByKey(p, groupBy), Collectors.toList()));

			return map.values().stream().flatMap(x -> x.stream()).collect(Collectors.toList());
		} catch (Exception ex) {
			System.err.println(
					String.format("Failed to execute GROUPBY operation on Table[%s]. Check your params", tableName));
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#crossJoin(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public Set<JoinTuple> crossJoin(String tableName1, String tableName2) {
		Set<JoinTuple> joinTuples = new LinkedHashSet<>();
		try {
			Table table1 = tables.get(tableName1.toLowerCase());
			Table table2 = tables.get(tableName2.toLowerCase());

			Collection<Tuple> tuples1 = table1.getTuples().values();
			Collection<Tuple> tuples2 = table2.getTuples().values();

			for (Tuple tuple1 : tuples1) {
				for (Tuple tuple2 : tuples2) {
					JoinTuple joinTuple = new JoinTuple();
					addTupleToCrossTuple(table1.getName(), tuple1, joinTuple);
					addTupleToCrossTuple(table2.getName(), tuple2, joinTuple);
					joinTuples.add(joinTuple);
				}
			}
		} catch (Exception e) {
			System.err.println(String.format(
					"Failed to execute CROSS JOIN operation on Table1[%s] and Table2[%s]. Check your params",
					tableName1, tableName2));
		}

		return joinTuples;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#naturalJoin(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public Set<JoinTuple> naturalJoin(String tableName1, String tableName2) {
		Set<JoinTuple> naturalJoinTuples = new LinkedHashSet<>();

		try {
			Table table1 = tables.get(tableName1.toLowerCase());
			Table table2 = tables.get(tableName2.toLowerCase());

			ForeignKey foreignKey1 = table1.getForeignKey();
			if (foreignKey1 == null) {
				System.err.println(
						String.format("Failed to do NATURAL JOIN Table[%s] does not have any foreign key", tableName1));
				return null;
			}

			Collection<Tuple> tuples1 = table1.getTuples().values();

			for (Tuple tuple1 : tuples1) {
				// value foreign value of the foreign key in first table
				String foreingKeyValueInTuple1 = tuple1.getValue(foreignKey1.getKey());

				// find the tuple that matches the value of the foreign key in
				// table2
				Tuple tuple2 = table2.getTuple(foreingKeyValueInTuple1);

				JoinTuple joinTuple = new JoinTuple();
				addTupleToCrossTuple(table1.getName(), tuple1, joinTuple);
				if (tuple2 != null) {
					addTupleToCrossTuple(table2.getName(), tuple2, joinTuple);
					naturalJoinTuples.add(joinTuple);
				}
			}
		} catch (Exception e) {
			System.err.println(String.format(
					"Failed to execute NATURAL JOIN operation on Table1[%s] and Table2[%s]. Check your params",
					tableName1, tableName2));
		}
		return naturalJoinTuples;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#join(java.lang.String,
	 * java.lang.String, edu.ccny.db.project.JoinCondition)
	 */
	@Override
	public Set<JoinTuple> join(String tableName1, String tableName2, JoinCondition joinCondition) {

		Set<JoinTuple> joinTuples = new LinkedHashSet<>();

		try {
			Table table1 = tables.get(tableName1.toLowerCase());
			Table table2 = tables.get(tableName2.toLowerCase());

			Collection<Tuple> tuples1 = table1.getTuples().values();

			for (Tuple tuple1 : tuples1) {
				// find value of the join column of the first table
				String valueOnFistTable = tuple1.getValue(joinCondition.getColumnOfFirstTable());

				// find the tuple in the second table that matches the value of
				// the
				// first table and second table join column
				Tuple tuple2 = table2.getTuple(joinCondition.getColumnOfSecondTable(), valueOnFistTable);

				JoinTuple joinTuple = new JoinTuple();
				addTupleToCrossTuple(table1.getName(), tuple1, joinTuple);
				if (tuple2 != null) {
					addTupleToCrossTuple(table2.getName(), tuple2, joinTuple);
					joinTuples.add(joinTuple);
				}
			}
		} catch (Exception e) {
			System.err.println(
					String.format("Failed to execute JOIN operation on Table1[%s] and Table2[%s]. Check your params",
							tableName1, tableName2));

		}
		return joinTuples;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#union(java.util.Set, java.util.Set)
	 */
	@Override
	public Set<Tuple> union(Set<Tuple> tuples1, Set<Tuple> tuples2) {
		try {
			Set<Tuple> tuples = SetUtil.union(tuples1, tuples2);
			return tuples;
		} catch (Exception e) {
			System.err.println("Failed to execute UION operation. Check your params");
			return null;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#intersection(java.util.Set,
	 * java.util.Set)
	 */
	@Override
	public Set<Tuple> intersection(Set<Tuple> tuples1, Set<Tuple> tuples2) {
		try {
			Set<Tuple> tuples = SetUtil.intersection(tuples1, tuples2);
			return tuples;
		} catch (Exception e) {
			System.err.println("Failed to execute INTERSECTION operation. Check your params");
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.ccny.db.project.DBService#difference(java.util.Set,
	 * java.util.Set)
	 */
	@Override
	public Set<Tuple> difference(Set<Tuple> tuples1, Set<Tuple> tuples2) {
		try {
			Set<Tuple> tuples = SetUtil.difference(tuples1, tuples2);
			return tuples;
		} catch (Exception e) {
			System.err.println("Failed to execute DIFFERENCE operation. Check your params");
			return null;
		}
	}

	@Override
	public void delete(String tableName) {
		try {
			Table table = tables.get(tableName.toLowerCase());
			table.deleteAll();
		} catch (Exception e) {
			System.err.println("Failed to execute DELETE operation. Check your params");
		}

	}

	@Override
	public void delete(String tableName, List<Condition> conditions, LOGICAL logicType) {
		Table table = tables.get(tableName.toLowerCase());
		Set<Tuple> tuples = select(tableName, conditions, logicType);
		table.delete(tuples);
	}

	@Override
	public void printJoinTuples(List<JoinTuple> tuples) {
		boolean isFirstTuple = true;
		for (JoinTuple joinTule : tuples) {
			if (isFirstTuple) {
				System.out.println(joinTule.getHeaderString());
				isFirstTuple = false;
			}
			System.out.println(joinTule.getStringValues());
		}

	}
	
	@Override
	public void printJoinTuples(Set<JoinTuple> tuples) {
		boolean isFirstTuple = true;
		for (JoinTuple joinTule : tuples) {
			if (isFirstTuple) {
				System.out.println(joinTule.getHeaderString());
				isFirstTuple = false;
			}
			System.out.println(joinTule.getStringValues());
		}

	}

	@Override
	public void printTable(String tableName) {
		Table table = getTable(tableName);
		List<Tuple> tuples = table.getTuples().values().stream().collect(Collectors.toList());
		printTuples(tuples);

	}

	
	@Override
	public void printTuples(Set<Tuple> tuples) {
		printTuples(tuples.stream().collect(Collectors.toList()));
		
	}
	
	@Override
	public void printTuples(List<Tuple> tuples) {
		boolean isFirstTuple = true;
		for (Tuple tuple : tuples) {
			if (isFirstTuple) {
				System.out.println(tuple.getHeaderString());
				isFirstTuple = false;
			}
			System.out.println(tuple.getStringValues());
		}
	}

	private void addTupleToCrossTuple(String tableName, Tuple tuple, JoinTuple crossTuple) {
		for (Column column : tuple.getValues().values()) {
			String key = String.format("%s.%c", tableName, column.getName());
			crossTuple.add(key, column);
		}
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
