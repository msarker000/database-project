package edu.ccny.test;

import static org.junit.Assert.*;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

import edu.ccny.db.project.DBOperator;
import edu.ccny.db.project.DBService;
import edu.ccny.db.project.ForeignKey;
import edu.ccny.db.project.JoinCondition;
import edu.ccny.db.project.JoinTuple;
import edu.ccny.db.project.Table;

public class DBOpertatorJoinTest {

	@Test
	public void test() {
		// student table
		Table departmentTable = new Table("Department");

		departmentTable.addColumn('N', "string"); // name of the department
		departmentTable.addColumn('D', "string"); // dean of the department
		departmentTable.addColumn('I', "integer"); // department code/ID

		// add primary key
		Set<Character> departmentKey = new LinkedHashSet<>();
		departmentKey.add('I');
		departmentTable.addPrimaryKey(departmentKey, "");

		departmentTable.insert("DSE", "Phiphes D Costa", "1004");
		departmentTable.insert("CSE", "Winct Silva", "1001");
		departmentTable.insert("CHSE", "Michele Grosbug", "1002");
		departmentTable.insert("PHSE", "Jhon Mike", "1003");

		// student table
		Table studentTable = new Table("Student");
		studentTable.addColumn('N', "string"); // name
		studentTable.addColumn('A', "integer"); // age
		studentTable.addColumn('R', "integer"); // roll
		studentTable.addColumn('C', "integer"); // department Code to correspond
												// to ID

		// add primary key
		Set<Character> studentkey = new LinkedHashSet<>();
		studentkey.add('R');
		studentTable.addPrimaryKey(studentkey, "");

		DBService database = new DBOperator();
		database.addTable(studentTable);
		database.addTable(departmentTable);

		studentTable.insert("Tom3", "32", "110", "1004");
		studentTable.insert("Ayub", "35", "100", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");

		JoinCondition joinCondition = new JoinCondition(studentTable.getColumn('C'), departmentTable.getColumn('I'));

		Set<JoinTuple> joinTuples = database.join("Student", "Department", joinCondition);
		assertEquals(3, joinTuples.size());

		studentTable.insert("Calos", "37", "120", "1003");
		joinTuples = database.join("Student", "Department", joinCondition);
		assertEquals(4, joinTuples.size());

		studentTable.insert("Tony", "37", "140", "1010");
		joinTuples = database.join("Student", "Department", joinCondition);
		assertEquals(4, joinTuples.size());

		boolean isFirstTuple = true;
		for (JoinTuple joinTule : joinTuples) {
			if (isFirstTuple) {
				System.out.println(joinTule.getHeaderString());
				isFirstTuple = false;
			}
			System.out.println(joinTule.getStringValues());
		}
	}

}
