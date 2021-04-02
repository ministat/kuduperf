package org.apache.kudu.examples;

import org.apache.kudu.examples.threading.*;

public class AsyncFuture {
    public static void main(String[] args) {
        stressTest2("Test");
    }

    public static void stressTest2(Object obj) {
        StressExecutors se = new StressExecutors(6, 4, 60000);
        se.run((i) -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return System.currentTimeMillis() + " " + i + " finished";
        }, (s) -> {
            System.out.println(s);
        });
    }

}
