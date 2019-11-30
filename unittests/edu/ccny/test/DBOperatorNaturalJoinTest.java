package edu.ccny.test;

import static org.junit.Assert.*;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

import edu.ccny.db.project.DBOperator;
import edu.ccny.db.project.DBService;
import edu.ccny.db.project.ForeignKey;
import edu.ccny.db.project.JoinTuple;
import edu.ccny.db.project.Table;

public class DBOperatorNaturalJoinTest {

	@Test
	public void testNaturalJoinTest() {
		// student table
		Table departmentTable = new Table("Department");
		departmentTable.addColumn('N', "string"); // name of the department
		departmentTable.addColumn('D', "string"); // dean of the department
		departmentTable.addColumn('C', "integer"); // department code

		// add primary key
		Set<Character> departmentKey = new LinkedHashSet<>();
		departmentKey.add('C');
		departmentTable.addPrimaryKey(departmentKey,"");

		departmentTable.insert("DSE", "Phiphes D Costa", "1004");
		departmentTable.insert("CSE", "Winct Silva", "1001");
		departmentTable.insert("CHSE", "Michele Grosbug", "1002");
		departmentTable.insert("PHSE", "Jhon Mike", "1003");
		
		
		assertEquals(4, departmentTable.getTuples().values().size());

		// student table
		Table studentTable = new Table("Student");
		studentTable.addColumn('N', "string"); // name
		studentTable.addColumn('A', "integer"); // age
		studentTable.addColumn('R', "integer"); // roll
		studentTable.addColumn('C', "integer"); // department Code

		// add primary key
		Set<Character> studentkey = new LinkedHashSet<>();
		studentkey.add('R');
		studentTable.addPrimaryKey(studentkey,"");

		Set<Character> forKey = new LinkedHashSet<>();
		forKey.add('C');
		ForeignKey foreignKey = new ForeignKey(departmentTable, forKey,"");
		studentTable.addForeignKey(foreignKey);

		DBService dbOperator = new DBOperator();
		dbOperator.addTable(studentTable);
		dbOperator.addTable(departmentTable);

		studentTable.insert("Tom3", "32", "110", "1004");
		studentTable.insert("Ayub", "35", "100", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");
		
		assertEquals(3, studentTable.getTuples().values().size());

		Set<JoinTuple> joinTuples = dbOperator.naturalJoin("Student", "Department");
		assertEquals(3, joinTuples.size());

		studentTable.insert("Calos", "37", "120", "1003");
		joinTuples = dbOperator.naturalJoin("Student", "Department");
		assertEquals(4, joinTuples.size());

		studentTable.insert("Tony", "37", "140", "1003");
		joinTuples = dbOperator.naturalJoin("Student", "Department");
		assertEquals(5, joinTuples.size());

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
