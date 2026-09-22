package com.issaalsabeh.etl.monitoring;

public interface HealthCheck {

    String name();

    boolean isCritical();

    void check() throws Exception;
}