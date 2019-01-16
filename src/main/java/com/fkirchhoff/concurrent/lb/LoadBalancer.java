package com.fkirchhoff.concurrent.lb;

import java.util.function.Function;

public interface LoadBalancer<T> {
    T next();

    void disableNode(T node);

    void retryDisabledNodes(Function<T, Boolean> test);
}
