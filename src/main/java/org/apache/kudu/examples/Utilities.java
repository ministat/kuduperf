package org.apache.kudu.examples;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class Utilities {
    public static String convertExceptionMessage(Exception e) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String utf8 = StandardCharsets.UTF_8.name();
        try {
            PrintStream ps = new PrintStream(baos, true, utf8);
            e.printStackTrace(ps);
            return baos.toString();
        } catch (Exception ex) {
            return null;
        }
    }
}
