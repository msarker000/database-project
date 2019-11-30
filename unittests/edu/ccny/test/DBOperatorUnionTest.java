package edu.ccny.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ccny.db.project.Condition;
import edu.ccny.db.project.Constraint;
import edu.ccny.db.project.DBOperator;
import edu.ccny.db.project.DBService;
import edu.ccny.db.project.Operator;
import edu.ccny.db.project.Table;
import edu.ccny.db.project.Tuple;

public class DBOperatorUnionTest {

	@Test
	public void testUnionTest() {
		Table studentTable = new Table("Student");
		studentTable.addColumn('N', "string"); // name
		studentTable.addColumn('A', "integer"); // age
		studentTable.addColumn('R', "integer"); // roll
		studentTable.addColumn('C', "integer"); // department Code

		// add primary key
		Set<Character> studentkey = new LinkedHashSet<>();
		studentkey.add('R');
		studentTable.addPrimaryKey(studentkey, "");

		// add constraints
		studentTable.addConstrain(new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL"));
		studentTable.addConstrain(new Constraint('A', Operator.LESS_THAN_EQUAL, "100", "A_LESS_THAN_EQUAL"));
		studentTable.addConstrain(new Constraint('R', Operator.GRREATER_THAN_EQUAL, "100", "R_GRREATER_THAN_EQUAL"));
		studentTable.addConstrain(new Constraint('R', Operator.LESS_THAN_EQUAL, "200", "R_LESS_THAN_EQUAL"));

		studentTable.insert("Ayub", "35", "100", "1001");
		studentTable.insert("Jhon", "29", "102", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");
		studentTable.insert("Yakub", "32", "150", "1003");
		studentTable.insert("Mikchel", "36", "155", "1003");
		studentTable.insert("Tony", "31", "120", "1004");
		studentTable.insert("Tom", "34", "130", "1004");

		assertEquals(7, studentTable.getTuples().values().size());

		List<Condition> conditions = new ArrayList<>();
		conditions.add(new Condition('R', Operator.GRREATER_THAN, "100",""));
		conditions.add(new Condition('R', Operator.LESS_THAN, "130",""));

		DBService dbOperator = new DBOperator();
		dbOperator.addTable(studentTable);
		Set<Tuple> tuples1 = dbOperator.select("Student", conditions, DBService.LOGICAL.AND);
		printTuples(tuples1);

		conditions.clear();
		conditions.add(new Condition('C', Operator.EQUAL, "1001",""));
		conditions.add(new Condition('C', Operator.EQUAL, "1004",""));

		System.out.println("-------------------------------");
		Set<Tuple> tuples2 = dbOperator.select("Student", conditions, DBService.LOGICAL.OR);
		printTuples(tuples2);

		System.out.println("--------------union of two sets of tuple-----------------");
		Set<Tuple> unionTuples = dbOperator.union(tuples1, tuples2);
		printTuples(unionTuples);

		assertEquals(5, unionTuples.size());

	}

	private void printTuples(Set<Tuple> tuples) {
		boolean isFirstTuple = true;
		for (Tuple tuple : tuples) {
			if (isFirstTuple) {
				System.out.println(tuple.getHeaderString());
				isFirstTuple = false;
			}
			System.out.println(tuple.getStringValues());
		}
	}

}
