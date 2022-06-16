package com.github.jshaptic.project;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystemNotFoundException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Some class test description.
 */
public class SomeClassTest extends Assert {

  @Test
  public void someMethodReturnsTrue() {
    SomeClass classUnderTest = new SomeClass();
    assertTrue(classUnderTest.someMethod(), "someExampleMethod should return 'true'");
  }

  @Test(expectedExceptions = FileSystemNotFoundException.class)
  public void testReadResourceAsUri() throws IOException, URISyntaxException {
    SomeClass classUnderTest = new SomeClass();
    assertEquals(classUnderTest.readResourceAsUri(), "some data");
  }

  @Test
  public void testReadResourceAsStream() throws IOException {
    SomeClass classUnderTest = new SomeClass();
    assertEquals(classUnderTest.readResourceAsStream(), "some data");
  }

}
