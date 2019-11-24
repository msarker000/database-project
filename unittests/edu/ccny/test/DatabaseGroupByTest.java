package edu.ccny.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import edu.ccny.db.project.Constraint;
import edu.ccny.db.project.Database;
import edu.ccny.db.project.Operator;
import edu.ccny.db.project.Table;
import edu.ccny.db.project.Tuple;

public class DatabaseGroupByTest {

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

		studentTable.insert("Tom3", "32", "110", "1004");
		studentTable.insert("Ayub", "35", "100", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");
		studentTable.insert("Yakub", "32", "150", "1003");
		studentTable.insert("Tom", "32", "120", "1004");
		studentTable.insert("Tom2", "32", "130", "1004");
		studentTable.insert("Ayub2", "35", "123", "1001");
		studentTable.insert("Tony", "37", "115", "1002");
		assertEquals(8, studentTable.getTuples().values().size());

		Database database = new Database();
		database.addTable(studentTable);
		Set<Character> groupByChars = new LinkedHashSet<>();
		groupByChars.add('C');
		Map<String, List<Tuple>> map = database.groupBy(studentTable, groupByChars); // group
																			// by
																			// department
																			// code
		System.out.println(map);
		assertEquals(3,map.get("1004").size());
		assertEquals(2,map.get("1001").size());
		assertEquals(2,map.get("1002").size());
		assertEquals(1,map.get("1003").size());
		
		
		groupByChars.clear();
		groupByChars.add('C');
		groupByChars.add('A');
	    map = database.groupBy(studentTable, groupByChars); // group
															// by
															// department
															// code and Age
		System.out.println(map);
		
		assertEquals(1,map.get("100332").size());
		assertEquals(2,map.get("100135").size());
		assertEquals(3,map.get("100432").size());
		assertEquals(2,map.get("100237").size());
	}

}
