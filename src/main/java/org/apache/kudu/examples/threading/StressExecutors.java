package org.apache.kudu.examples.threading;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class StressExecutors {
    private int threads;
    private int durMs;
    private Semaphore semaphore;
    private MyFutureExecutor executor;
    private ExecutorService service;

    public StressExecutors(int threads,
                           int parallelTasks,
                           int durMs) {
        this.threads = threads;
        this.durMs = durMs;

        service = Executors.newFixedThreadPool(threads);
        executor = new MyFutureExecutor(service);
        semaphore = new Semaphore(parallelTasks);
    }

    public boolean run(MyCallable call, IReceiveResult resultReceiver) {
        long s = System.currentTimeMillis();
        long e = s + durMs;
        for (int i = 0; e > s; i++) {
            try {
                semaphore.acquire();
                s = System.currentTimeMillis();
                ListenableFuture<String> future = executor.submit(call, i);
                future.addCallback(new ResultPrinter((r) -> {
                    resultReceiver.ReceiveResult(r);
                    semaphore.release();
                }));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        service.shutdown();
        return true;
    }
}
