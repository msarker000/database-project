package edu.ccny.test;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

import edu.ccny.db.project.Constraint;
import edu.ccny.db.project.DeleteAction;
import edu.ccny.db.project.ForeignKey;
import edu.ccny.db.project.Operator;
import edu.ccny.db.project.Table;

public class TableTest {

	@Test
	public void testDeleteIntoRelationalTable() {
		Table departmentTable = new Table("Department");
		departmentTable.addColumn('N', "string"); // name of department
		departmentTable.addColumn('C', "integer"); // code of the department
		Set<Character> deptkey = new LinkedHashSet<>();
		deptkey.add('C');
		departmentTable.addPrimaryKey(deptkey, "");
		departmentTable.addConstrain(new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_NULL"));

		//
		departmentTable.insert("CSE", "1001");
		departmentTable.insert("DSE", "1002");
		departmentTable.insert("APSE", "1003");
		departmentTable.insert("CHME", "1004");
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

		// foreignKey
		Set<Character> forKey = new LinkedHashSet<>();
		forKey.add('C');
		ForeignKey foreignKey = new ForeignKey(departmentTable, forKey, "");
		studentTable.addForeignKey(foreignKey);

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

		studentTable.delete("100");
		assertEquals(4, studentTable.getTuples().values().size());
		assertEquals(4, departmentTable.getTuples().values().size());

		studentTable.delete("120");
		assertEquals(3, studentTable.getTuples().values().size());
		assertEquals(4, departmentTable.getTuples().values().size());

		studentTable.insert("Jenny2", "37", "102", "1002");
		studentTable.insert("Jenny1", "37", "103", "1002");

		assertEquals(5, studentTable.getTuples().values().size());
		assertEquals(4, departmentTable.getTuples().values().size());
		studentTable.printTuples();
		departmentTable.printTuples();
		
		foreignKey.setAction(DeleteAction.SET_NULL);
		departmentTable.delete("1002");
		
		assertEquals(3, departmentTable.getTuples().values().size());
		assertEquals(5, studentTable.getTuples().values().size());
		studentTable.printTuples();
		departmentTable.printTuples();
		
		studentTable.delete("101");
		
		studentTable.printTuples();
	}

	@Test
	public void testDeleteCascadeIntoRelationalTable() {
		Table departmentTable = new Table("Department");
		departmentTable.addColumn('N', "string"); // name of department
		departmentTable.addColumn('C', "integer"); // code of the department
		Set<Character> deptkey = new LinkedHashSet<>();
		deptkey.add('C');
		departmentTable.addPrimaryKey(deptkey, "");
		departmentTable.addConstrain(new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL"));

		//
		departmentTable.insert("CSE", "1001");
		departmentTable.insert("DSE", "1002");
		departmentTable.insert("APSE", "1003");
		departmentTable.insert("CHME", "1004");
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

		// foreignKey
		Set<Character> forKey = new LinkedHashSet<>();
		forKey.add('C');
		ForeignKey foreignKey = new ForeignKey(departmentTable, forKey, "");
		studentTable.addForeignKey(foreignKey);

		Constraint constraint1 = new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL");
		Constraint constraint2 = new Constraint('A', Operator.LESS_THAN_EQUAL, "100", "A_LESS_THAN_EQUAL");
		Constraint constraint3 = new Constraint('R', Operator.GRREATER_THAN_EQUAL, "100", "R_GRREATER_THAN_EQUAL");
		Constraint constraint4 = new Constraint('R', Operator.LESS_THAN_EQUAL, "200", "R_LESS_THAN_EQUAL");

		studentTable.insert("Ayub", "35", "100", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");
		studentTable.insert("Yakub", "32", "150", "1003");
		studentTable.insert("Tom", "32", "120", "1004");
		studentTable.insert("Tom2", "32", "130", "1004");
		assertEquals(5, studentTable.getTuples().values().size());

		studentTable.insert("Jenny2", "37", "102", "1002");
		studentTable.insert("Jenny1", "37", "103", "1002");

		assertEquals(7, studentTable.getTuples().values().size());
		assertEquals(4, departmentTable.getTuples().values().size());

		foreignKey.setAction(DeleteAction.CASCADE);

		departmentTable.delete("1002");
		assertEquals(3, departmentTable.getTuples().values().size());
		assertEquals(4, studentTable.getTuples().values().size());
		studentTable.printTuples();
	}

}
