package org.apache.kudu.examples.threading;

public class ListenableFuture<V> {
    private IFutureCallback<V> callback;
    private V result;
    private Throwable failure;
    private boolean isCompleted;

    public void addCallback(IFutureCallback<V> callback) {
        this.callback = callback;
        resolve();
    }

    public void setResult(V result) {
        this.result = result;
        isCompleted = true;
        resolve();
    }

    public void setFailure(Throwable failure) {
        this.failure = failure;
        isCompleted = true;
        resolve();
    }

    private void resolve() {
        if (callback != null && isCompleted) {
            if (failure == null) {
                callback.onSuccess(result);
            } else {
                callback.onFailure(failure);
            }
        }
    }
}