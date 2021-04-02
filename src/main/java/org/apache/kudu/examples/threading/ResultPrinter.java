package org.apache.kudu.examples.threading;

public class ResultPrinter<V> implements IFutureCallback<V> {
    private IReceiveResult _r;
    public ResultPrinter(IReceiveResult r) {
        _r = r;
    }

    @Override
    public void onSuccess(V result) {
        _r.ReceiveResult(result);
    }

    @Override
    public void onFailure(Throwable failure) {
        failure.printStackTrace();
        _r.ReceiveResult(null);
    }
}
