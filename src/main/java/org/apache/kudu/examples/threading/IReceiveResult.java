package org.apache.kudu.examples.threading;

public interface IReceiveResult<V> {
    void ReceiveResult(V r);
}
