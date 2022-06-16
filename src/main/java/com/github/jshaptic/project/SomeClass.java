package com.github.jshaptic.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Some class description.
 */
public class SomeClass {

  private String resource = "some-resource.txt";

  /** some method */
  public boolean someMethod() {
    return true;
  }

  /** some untested method */
  public boolean someUntestedMethod() {
    return true;
  }

  /** read resource as uri */
  public String readResourceAsUri() throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemClassLoader().getResource(resource).toURI())));
  }

  /** read resource as stream */
  public String readResourceAsStream() throws IOException {
    try (InputStream in = ClassLoader.getSystemResourceAsStream(resource);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));) {
      return reader.readLine();
    }
  }

}
