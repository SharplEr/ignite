/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.OK;

/** */
public class GZipCompressEngine implements CompressEngine {
    /** */
    private boolean isInboundDone = false;

    private long bytesBefore = 0;
    private long bytesAfter = 0;

    /** */
    public CompressEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        byte[] bytes = new byte[src.remaining()];

        src.get(bytes);
        bytesBefore += bytes.length;

        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream out = new GZIPOutputStream(baos);
        ) {
            out.write(bytes);
            out.close(); // need it, otherwise EOFException at decompressing

            bytes = baos.toByteArray();
        }

        bytes = concat(toArray(bytes.length), bytes);

        if (bytes.length > buf.remaining())
            return BUFFER_OVERFLOW;

        buf.put(bytes);
        bytesAfter += bytes.length;

        return OK;
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    /** */
    public boolean isInboundDone() {
        return isInboundDone;
    }

    /** */
    public void closeInbound() throws IOException{
        //No-op
        System.out.println("MY bytesBefore:"+bytesBefore+" bytesAfter;"+bytesAfter+ " cr="+bytesBefore*1.0/bytesAfter);
    }

    /** */
    public CompressEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        if (src.remaining() == 0)
            return BUFFER_UNDERFLOW;

        int initPos = src.position();

        if (src.remaining() <= 5) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        byte[] lenBytes = new byte[4];
        src.get(lenBytes);
        int len = toInt(lenBytes);

        if (src.remaining() < len) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        byte[] bytes = new byte[len]; // ? ByteArrayInputStream->ByteBufferInputStream

        src.get(bytes);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             InputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))
        ) {
            byte[] buffer = new byte[32];
            int length;

            while ((length = in.read(buffer)) != -1)
                baos.write(buffer, 0, length);

            baos.flush();

            byte[] output = baos.toByteArray();

            if (output.length > buf.remaining()) {
                src.position(initPos);

                return BUFFER_OVERFLOW;
            }

            buf.put(output);
        }

        if (src.remaining() == 0)
            return BUFFER_UNDERFLOW;

        return OK;
    }

    /** */
    private int toInt(byte[] bytes){
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16)
            | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    /** */
    private byte[] toArray(int val){
        return  new byte[] {
            (byte)(val >>> 24),
                (byte)(val >>> 16),
                (byte)(val >>> 8),
                (byte)val
            };
    }
}

