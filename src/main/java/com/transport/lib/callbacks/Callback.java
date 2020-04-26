package com.transport.lib.callbacks;

/*
    Represents Callbacks for async invocations
 */
public interface Callback<T> {

    // Will be invoked if server-side method finished successfully
    void onSuccess(String key, T result);

    // Server-side method invocation method finished with exception
    void onError(String key, Throwable exception);
}
