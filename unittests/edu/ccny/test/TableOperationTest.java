package edu.ccny.test;

import static org.junit.Assert.*;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

import edu.ccny.db.project.Constraint;
import edu.ccny.db.project.DeleteAction;
import edu.ccny.db.project.ForeignKey;
import edu.ccny.db.project.Operator;
import edu.ccny.db.project.Table;

public class TableOperationTest {

	@Test
	public void testInsertPrivaryKeyViolation() {

		Table studentTable = new Table("Student");
		studentTable.addColumn('N', "string"); // name
		studentTable.addColumn('A', "integer"); // age
		studentTable.addColumn('R', "integer"); // roll

		Set<Character> key = new LinkedHashSet<>();
		key.add('R');

		studentTable.addPrimaryKey(key, "");

		studentTable.insert("Ayub", "35", "100");
		studentTable.insert("Jenny", "37", "101");
		studentTable.insert("Yakub", "32", "100");

		studentTable.printTuples();

		assertEquals(2, studentTable.getTuples().values().size());

	}

	@Test
	public void testInsertWithConstraint1() {

		Table studentTable = new Table("Student");
		studentTable.addColumn('N', "string"); // name
		studentTable.addColumn('A', "integer"); // age
		studentTable.addColumn('R', "integer"); // roll

		Constraint constraint1 = new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL");
		Constraint constraint2 = new Constraint('A', Operator.LESS_THAN_EQUAL, "100", "A_LESS_THAN_EQUAL");
		Constraint constraint3 = new Constraint('R', Operator.GRREATER_THAN_EQUAL, "100", "R_GRREATER_THAN_EQUAL");
		Constraint constraint4 = new Constraint('R', Operator.LESS_THAN_EQUAL, "200", "R_LESS_THAN_EQUAL");

		Set<Character> key = new LinkedHashSet<>();
		key.add('R');

		studentTable.addPrimaryKey(key, "");
		studentTable.addConstrain(constraint1);
		studentTable.addConstrain(constraint2);
		studentTable.addConstrain(constraint3);
		studentTable.addConstrain(constraint4);

		studentTable.insert("Ayub", "35", "100");
		studentTable.insert("Jenny", "37", "101");
		studentTable.insert("Yakub", "32", "150");
		studentTable.insert(null, "32", "120");

		studentTable.printTuples();

		assertEquals(3, studentTable.getTuples().values().size());

	}

	@Test
	public void testInsertWithConstraint2() {

		Table studentTable = new Table("Student");
		studentTable.addColumn('N', "string"); // name
		studentTable.addColumn('A', "integer"); // age
		studentTable.addColumn('R', "integer"); // roll
		
		Constraint constraint1 = new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL");
		Constraint constraint2 = new Constraint('A', Operator.LESS_THAN_EQUAL, "100", "A_LESS_THAN_EQUAL");
		Constraint constraint3 = new Constraint('R', Operator.GRREATER_THAN_EQUAL, "100", "R_GRREATER_THAN_EQUAL");
		Constraint constraint4 = new Constraint('R', Operator.LESS_THAN_EQUAL, "200", "R_LESS_THAN_EQUAL");

		Set<Character> key = new LinkedHashSet<>();
		key.add('R');

		studentTable.addPrimaryKey(key, "");
		studentTable.addConstrain(constraint1);
		studentTable.addConstrain(constraint2);
		studentTable.addConstrain(constraint3);
		studentTable.addConstrain(constraint4);

		studentTable.insert("Ayub", "35", "100");
		studentTable.insert("Jenny", "37", "101");
		studentTable.insert("Yakub", "32", "150");
		studentTable.insert("Tom", "32", "220");

		studentTable.printTuples();

		assertEquals(3, studentTable.getTuples().values().size());

	}

	@Test
	public void testIDeleteFromTable() {

		Table studenTable = new Table("Student");
		studenTable.addColumn('N', "string"); // name
		studenTable.addColumn('A', "integer"); // age
		studenTable.addColumn('R', "integer"); // roll

		Constraint constraint1 = new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL");
		Constraint constraint2 = new Constraint('A', Operator.LESS_THAN_EQUAL, "100", "A_LESS_THAN_EQUAL");
		Constraint constraint3 = new Constraint('R', Operator.GRREATER_THAN_EQUAL, "100", "R_GRREATER_THAN_EQUAL");
		Constraint constraint4 = new Constraint('R', Operator.LESS_THAN_EQUAL, "200", "R_LESS_THAN_EQUAL");

		Set<Character> key = new LinkedHashSet<>();
		key.add('R');

		studenTable.addPrimaryKey(key,"");
		studenTable.addConstrain(constraint1);
		studenTable.addConstrain(constraint2);
		studenTable.addConstrain(constraint3);
		studenTable.addConstrain(constraint4);

		studenTable.insert("Ayub", "35", "100");
		studenTable.insert("Jenny", "37", "101");
		studenTable.insert("Yakub", "32", "150");

		studenTable.printTuples();

		assertEquals(3, studenTable.getTuples().values().size());

		studenTable.delete("100");
		assertEquals(2, studenTable.getTuples().values().size());

		studenTable.delete("101");
		assertEquals(1, studenTable.getTuples().values().size());

		studenTable.delete("150");
		assertEquals(0, studenTable.getTuples().values().size());
	}

	@Test
	public void testInsertIntoRelationalTable() {
		Table departmentTable = new Table("Department");
		departmentTable.addColumn('N', "string"); // name of department
		departmentTable.addColumn('C', "integer"); // code of the department
		Set<Character> deptkey = new LinkedHashSet<>();
		deptkey.add('C');
		departmentTable.addPrimaryKey(deptkey,"");
		departmentTable.addConstrain(new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL"));

		//
		departmentTable.insert("CSE", "1001");
		departmentTable.insert("DSE", "1002");
		departmentTable.insert("APSE", "1003");
		departmentTable.insert("CHME", "1004");

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

		studentTable.printTuples();

		assertEquals(4, studentTable.getTuples().values().size());

	}

	@Test
	public void testInsertIntoRelationalViolationTable() {

		Table departmentTable = new Table("Department");
		departmentTable.addColumn('N', "string"); // name of department
		departmentTable.addColumn('C', "integer"); // code of the department
		Set<Character> deptkey = new LinkedHashSet<>();
		deptkey.add('C');
		departmentTable.addPrimaryKey(deptkey,"");
		departmentTable.addConstrain(new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL"));

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

		// foreignKey
		Set<Character> forKey = new LinkedHashSet<>();
		forKey.add('C');
		ForeignKey foreignKey = new ForeignKey(departmentTable, forKey,"");
		studentTable.addForeignKey(foreignKey);

		Constraint constraint1 = new Constraint('N', Operator.NOT_EQUAL, null, "N_NOT_EQUAL");
		Constraint constraint2 = new Constraint('A', Operator.LESS_THAN_EQUAL, "100", "A_LESS_THAN_EQUAL");
		Constraint constraint3 = new Constraint('R', Operator.GRREATER_THAN_EQUAL, "100", "R_GRREATER_THAN_EQUAL");
		Constraint constraint4 = new Constraint('R', Operator.LESS_THAN_EQUAL, "200", "R_LESS_THAN_EQUAL");

		studentTable.insert("Ayub", "35", "100", "1001");
		studentTable.insert("Jenny", "37", "101", "1002");
		studentTable.insert("Yakub", "32", "150", "1003");
		studentTable.insert("Tom", "32", "120", "1004");

		studentTable.printTuples();

		assertEquals(0, studentTable.getTuples().values().size());

	}

}
