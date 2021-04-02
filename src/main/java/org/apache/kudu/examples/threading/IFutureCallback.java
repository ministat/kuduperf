package org.apache.kudu.examples.threading;

public interface IFutureCallback<V> {
    void onSuccess(V result);

    void onFailure(Throwable failure);
}