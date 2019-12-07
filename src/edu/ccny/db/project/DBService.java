package edu.ccny.db.project;

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ccny.db.project.DBService.LOGICAL;

public interface DBService {

	enum LOGICAL {
		AND, OR
	}

	void addTable(Table table);
	
	void dropTable(String tableName);
	
	void showTableList();
	

	/**
	 * Selects tuples based on conditions and Logical AND or Condition
	 * 
	 * @param tableName
	 *            the table name
	 * @param conditions
	 *            the list of condition
	 * @param logicType
	 *            the logic type AND or OR
	 * 
	 * @return list of tuples that satisfy the condition;
	 */
	Set<Tuple> select(String tableName, List<Condition> conditions, LOGICAL logicType);
	
	
	Set<Tuple> select(String tableName);

	/**
	 * Selects tuples with group
	 * 
	 * @param tableName
	 *            the name of table
	 * @param groupBy
	 *            the list of column attribute
	 * @return list of tuple with group
	 */
	List<Tuple> selectWithGroupBy(String tableName, Set<Character> groupBy);

	/**
	 * Selects tuples from two tables using Cross join
	 * 
	 * @param tableName1
	 *            the first table name
	 * @param tableName2
	 *            the second table name
	 * @return list of join tuple
	 */
	Set<JoinTuple> crossJoin(String tableName1, String tableName2);

	/**
	 * Selects tuples from two tables using natural join
	 * 
	 * @param tableName1
	 *            the first table name
	 * @param tableName2
	 *            the second table name
	 * @return list of join tuple
	 */
	Set<JoinTuple> naturalJoin(String tableName1, String tableName2);

	/**
	 * Selects tuples from two tables using join condition
	 * 
	 * @param tableName1
	 *            the first table name
	 * @param tableName2
	 *            the second table name
	 * @param joinCondition
	 *            the joinCondition of two table
	 * 
	 * @return list of join tuple
	 */
	Set<JoinTuple> join(String tableName1, String tableName2, JoinCondition joinCondition);

	/**
	 * finds union of two sets of tuple
	 * 
	 * @param tuples1
	 *            the first set of tuples
	 * @param tuples2
	 *            the second set of tuples
	 * 
	 * @return set of tuple those are in either tables
	 */
	Set<Tuple> union(Set<Tuple> tuples1, Set<Tuple> tuples2);

	/**
	 * finds intersection of two sets of tuple
	 * 
	 * @param tuples1
	 *            the first set of tuples
	 * @param tuples2
	 *            the second set of tuples
	 * 
	 * @return set of tuple those are in common in both table
	 */
	Set<Tuple> intersection(Set<Tuple> tuples1, Set<Tuple> tuples2);

	/**
	 * finds difference of two sets of tuple
	 * 
	 * @param tuples1
	 *            the first set of tuples
	 * @param tuples2
	 *            the second set of tuples
	 * 
	 * @return list of tuple those are in tuples1 but not in tuples2
	 * 
	 */
	Set<Tuple> difference(Set<Tuple> tuples1, Set<Tuple> tuples2);

	/**
	 * finds table for name
	 * 
	 * @param tableName
	 *            the tableName
	 * @return table
	 */
	Table getTable(String tableName);
	
	
	void printTuples(List<Tuple> tuples);
	
	void printTuples(Set<Tuple> tuples);
	
	void printTable(String tableName);
	
	void printJoinTuples(List<JoinTuple> tuples);
	
	void printJoinTuples(Set<JoinTuple> tuples);
	
	
	List<Tuple> selectWithGroupBy(List<Tuple> tuples, Set<Character> groupBy);
	
	
	void delete(String tableName);
	


	void delete(String tableName, List<Condition> conditions, LOGICAL logicType);
	

}