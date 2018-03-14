package org.apache.ignite.spi.communication;

import org.apache.ignite.internal.util.nio.compression.CompressionEngine;

import javax.cache.configuration.Factory;

/** */
public class ZstdFactory implements Factory<CompressionEngine> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Override public CompressionEngine create() {
        return new ZstdEngine();
    }
}
