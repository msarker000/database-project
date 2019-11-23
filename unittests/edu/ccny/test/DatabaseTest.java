package edu.ccny.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.xml.crypto.Data;

import org.junit.Test;

import edu.ccny.db.project.Condition;
import edu.ccny.db.project.Constraint;
import edu.ccny.db.project.Database;
import edu.ccny.db.project.ForeignKey;
import edu.ccny.db.project.Operator;
import edu.ccny.db.project.Table;
import edu.ccny.db.project.Tuple;

public class DatabaseTest {

	@Test
	public void test() {
		// student table
		Table studentTable = new Table("Student");
		studentTable.addColumn('N', "string"); // name
		studentTable.addColumn('A', "integer"); // age
		studentTable.addColumn('R', "integer"); // roll
		studentTable.addColumn('C', "integer"); // department Code

		// add primary key
		Set<Character> studentkey = new LinkedHashSet<>();
		studentkey.add('R');
		studentTable.addKey(studentkey);

		// add constraints
		studentTable.addConstrain(new Constraint('N', Operator.NOT_EQUAL, null));
		studentTable.addConstrain(new Constraint('A', Operator.LESS_THAN_EQUAL, "100"));
		studentTable.addConstrain(new Constraint('R', Operator.GRREATER_THAN_EQUAL, "100"));
		studentTable.addConstrain(new Constraint('R', Operator.LESS_THAN_EQUAL, "200"));

		studentTable.insert("Ayub", "35", "100", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");
		studentTable.insert("Yakub", "32", "150", "1003");
		studentTable.insert("Tom", "32", "120", "1004");
		studentTable.insert("Tom2", "32", "130", "1004");
		assertEquals(5, studentTable.getTuples().values().size());
		
		List<Condition> conditions = new ArrayList<>();
		
		conditions.add(new Condition('R', Operator.EQUAL, "100"));
		conditions.add(new Condition('N', Operator.EQUAL, "Ayub"));
		
		Database database = new Database();
		database.addTable(studentTable);
		List<Tuple> tuples =  database.select(studentTable, conditions);
		System.out.println(tuples);
		

	}

}
