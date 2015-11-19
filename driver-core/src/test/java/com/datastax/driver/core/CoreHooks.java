package com.datastax.driver.core;

/**
 * Exposes package-private methods to tests in other packages.
 */
public class CoreHooks {
    public static SimpleStatement newSimpleStatement(String query, ProtocolVersion protocolVersion, CodecRegistry codecRegistry, Object... values) {
        return new SimpleStatement(query, protocolVersion, codecRegistry, values);
    }
}
