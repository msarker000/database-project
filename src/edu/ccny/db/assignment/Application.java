package edu.ccny.db.assignment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * this class is the main class that is used to take user input and validates the user input
 * and find the answer based on user input 
 * 
 * 
 * @author ayub
 *
 */
public class Application {

	public static void main(String[] args) throws IOException {

		// Enter data using BufferReader
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		// Reading data using readLine
		System.out.println("******************************************************************************************");
		System.out.println("**********************************   Enter QUIT to exit    *******************************");
		System.out.println("******************************************************************************************");
		while (true) {
			// get Relational attributes
			Relation relation = getRelationAttributesFromUserInput(reader);
			
			// build relation with input string
			 addFDsToRelationFromUserInput(relation, reader);

			// printout Summary of Relations and FDS
			System.out.println("You entered relation and FDs:\n\tR(" + relation.toPrintableFormat()+")");
			for (FunctionDependency fd : relation.getOriginalFDs()) {
				System.out.println("\t" + fd.toPrintableFormat());
			}

			//show user question and answer based your input
			showQuestionAndAnswerToUser(reader, relation);

		}
	}

	private static void showQuestionAndAnswerToUser(BufferedReader reader, Relation relation) throws IOException {
		String questionStr = "";
		do {
			System.out.println("Enter 1: to find closures");
			System.out.println("      2: to find keys and NF");
			System.out.println("      #: to start with new relation and FDs");
			System.out.print("Option: ");
			questionStr = getUserInput(reader);
			if (questionStr.equals("1")) {
				//find closure of user input
				findClosureFromUserInput(reader, relation);

			} else if (questionStr.equals("2")) {
				// find keys and normalization
				findKeysAndNormalForm(relation);
			}
		} while (!questionStr.equals("#"));
	}

	private static void findKeysAndNormalForm(Relation relation) {
		DBUtil.findKeys(relation);
		DBUtil.findNormalForm(relation);
		System.out.print("\n\tKeys:\t");
		for(Set<Character> key: relation.getKeys()){
			System.out.print(setToPrintableString(key)+"\t");
		}
		System.out.println("\nNF of Relation:\t"+relation.getNormalForm().getName());
		
		System.out.println("\nNF of FDs:");
		for(FunctionDependency fd: relation.getOriginalFDs()){
			System.out.println("\t"+fd.toPrintableFormat() +" : "+ fd.getNormalForm().getName());
		}
		System.out.println();
	}

	private static void findClosureFromUserInput(BufferedReader reader, Relation relation)
			throws IOException {
		// find Close in loop
		System.out.println("Enter seed or # for back menu");
		String seedStr = "";
		do {
			System.out.print("Seed: ");
			seedStr = getUserInput(reader);
			if (!seedStr.equals("#")) {
				Set<Character> seedSet = SetUtil.getSetFromString(seedStr);
				if(isSeedContainsInRelation(relation, seedSet)){
					Set<Character>  closuresOfSeed =  DBUtil.findClosure(relation.getConvertedNonTrivialFDs(), seedSet);
					System.out.println(String.format("{%s}+ = %s", seedStr, setToPrintableString(closuresOfSeed)));
				}else{
					System.err.println("Wrong Seed. Seed is not Contain in Relation's attributes\n");	
				}
				
			}
		} while (!seedStr.equals("#"));
	}

	private static boolean isSeedContainsInRelation(Relation relation, Set<Character> seedSet) {	
		for(Character ch: seedSet){
			if(!relation.getAttributes().contains(ch)){
				return false;
			}
		}
		return true;
	}

	private static Relation getRelationAttributesFromUserInput(BufferedReader reader) throws IOException {
		String relationStr = "";

		boolean isValidRelation = true;
		do {
			System.out.print("Enter Relation's Attributes(In Uppercase): ");
			relationStr = getUserInput(reader);
			isValidRelation = relationStr.matches("[A-Z]*");
			if (!isValidRelation) {
				System.err.println("\nWrong attributes");
			}
		} while (!isValidRelation);

		Relation relation = new Relation(relationStr);
		return relation;
	}

	
	private static void addFDsToRelationFromUserInput(Relation relation, BufferedReader reader) throws IOException {
		System.out.println(">>>>Enter FDs in format X->Y. Enter # to finish entering FD<<<<\n");
		String fdStr = "";
		boolean isFDInputDone = false;
		
		do {
			boolean isValidFd = true;
			do {
				System.out.print("FD: ");
				fdStr = getUserInput(reader);
				if (fdStr.equalsIgnoreCase("#")) {
					isFDInputDone = true;
				} else {
					isValidFd = fdStr.matches("[A-Z]+->[A-Z]+");

					if (!isValidFd) {
						System.err.println("\nWrong format:Enter FD in X->Y format");
					} else {
						String[] fdStrs = fdStr.split("->");

						Set<Character> lhsOfFD = SetUtil.getSetFromString(fdStrs[0]);
						Set<Character> rhsOfFD = SetUtil.getSetFromString(fdStrs[1]);
						// IF FD not in the standard
						// non-trivial forms are changed to non-trivial ones,
						// e.g., AB->CD
						// is translated into standard non-trivial forms
						// AB->C
						// and AB->D
						if (isValidFD(relation, lhsOfFD, rhsOfFD)) {
							relation.addFD(lhsOfFD, rhsOfFD);
						}
					}
				}
			} while (!isValidFd || !isFDInputDone);
		} while (!isFDInputDone);
		
	}

	public static boolean isValidFD(Relation relation, Set<Character> lhsOfFD, Set<Character> rhsOfFD) {

		if (SetUtil.isSubset(lhsOfFD, rhsOfFD)) {
			System.err.println("\nWrong FD[RHS of FD is subset of LHS].");
			return false;

		}

		if (!relation.getAttributes().containsAll(lhsOfFD)) {
			System.err.println("\n Wrong FD[LHS contains attributes not in Relation].");
			return false;

		}

		if (!relation.getAttributes().containsAll(rhsOfFD)) {
			System.err.println("\n Wrong FD[RHS contains attributes not in Relation].");
			return false;
		}
		return true;
	}

	private static String getUserInput(BufferedReader reader) throws IOException {
		String userInput = "";
		do {
			userInput = reader.readLine();
			if (!userInput.isEmpty() && userInput.equalsIgnoreCase("quit")) {
				System.out.println("You entered QUIT to exit the application");
				System.exit(-1);
			}
		} while (userInput.isEmpty());

		return userInput;
	}

	public static String setToPrintableString(Set<Character> characterSet){
		return characterSet.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(""));
	}

}
