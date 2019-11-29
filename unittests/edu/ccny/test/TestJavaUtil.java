package edu.ccny.test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestJavaUtil {

	@Test
	public void test() {
		List<String> names = Arrays.asList();

		String name5 = names.stream().map(s -> s.toUpperCase()).filter(s -> s.length() < 5)
				.sorted((a, b) -> b.length() - a.length()).findFirst().orElse(null);
		System.out.println(name5);

		String name2 = names.stream().map(s -> s.toUpperCase()).filter(s -> s.length() < 2)
				.sorted((a, b) -> b.length() - a.length()).findFirst().orElse("Somthing else");
		System.out.println(name2);
	}

}
