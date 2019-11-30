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

public class DBOperatorInsertSelectTest {

	@Test
	public void testInsertIntoTable() {
		// student table
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
		studentTable.insert("Jenny", "37", "101", "1002");
		studentTable.insert("Yakub", "32", "150", "1003");
		studentTable.insert("Tom", "32", "120", "1004");
		studentTable.insert("Tom2", "32", "130", "1004");
		assertEquals(5, studentTable.getTuples().values().size());

		List<Condition> conditions = new ArrayList<>();

		conditions.add(new Condition('R', Operator.GRREATER_THAN, "100", "R_GRREATER_THAN_100"));
		conditions.add(new Condition('R', Operator.LESS_THAN, "130", "R_LESS_THAN_130"));
		// conditions.add(new Condition('N', Operator.NOT_EQUAL, "Ayub"));

		DBService dbOperator = new DBOperator();
		dbOperator.addTable(studentTable);
		Set<Tuple> tuples = dbOperator.select("Student", conditions, DBService.LOGICAL.AND);
		System.out.println(tuples);

		assertEquals(2, tuples.size());

		conditions.clear();
		conditions.add(new Condition('N', Operator.NOT_EQUAL, "Ayub", "N_NOT_EQUAL_ayub"));
		tuples = dbOperator.select("Student", conditions, DBService.LOGICAL.AND);
		System.out.println(tuples);
		assertEquals(4, tuples.size());

		conditions.clear();
		conditions.add(new Condition('N', Operator.EQUAL, "Ayub", "N_NOT_EQUAL_Ayub"));
		conditions.add(new Condition('N', Operator.EQUAL, "Tom", "N_NOT_EQUAL_Tom"));
		conditions.add(new Condition('N', Operator.EQUAL, "Tom2", "N_NOT_EQUAL_Tom2"));
		tuples = dbOperator.select("Student", conditions, DBService.LOGICAL.OR);
		System.out.println(tuples);
		assertEquals(3, tuples.size());

	}

}
