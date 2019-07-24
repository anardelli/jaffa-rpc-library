package com.transport.lib.common;

/*
    Represents Callbacks for async invocations
 */
@SuppressWarnings("all")
public interface Callback<T> {

    // Will be invoked if server-side method finished successfully
    public void onSuccess(String key, T result);

    // Server-side method invocation method finished with exception
    public void onError(String key, Throwable exception);
}
