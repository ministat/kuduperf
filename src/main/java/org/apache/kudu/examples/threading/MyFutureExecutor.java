package org.apache.kudu.examples.threading;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public class MyFutureExecutor {
    private ExecutorService executor;

    public MyFutureExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public ListenableFuture<String> submit(final MyCallable callable, final int index) {
        final ListenableFuture<String> future = new ListenableFuture<String>();
        executor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                    String result = callable.call(index);
                    future.setResult(result);
                    return result;
                } catch (Exception e) {
                    future.setFailure(e);
                    throw e;
                }
            }
        });

        return future;
    }
}
