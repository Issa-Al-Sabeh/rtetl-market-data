package com.issaalsabeh.etl.core;

public interface CommittableSource<T> extends Source<T> {

    void commit();
}
