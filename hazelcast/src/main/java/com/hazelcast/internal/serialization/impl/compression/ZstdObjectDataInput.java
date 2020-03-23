/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.compression;

import com.github.luben.zstd.ZstdInputStream;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.version.Version;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.UTF_8;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ZstdObjectDataInput extends ZstdInputStream implements CompressionObjectDataInput {

    private final InternalSerializationService service;
    private final ByteOrder byteOrder;
    private final byte[] numberBuffer = new byte[DOUBLE_SIZE_IN_BYTES];
    private Version version = Version.UNKNOWN;
    private Version wanProtocolVersion = Version.UNKNOWN;

    public ZstdObjectDataInput(InputStream in, InternalSerializationService service, ByteOrder byteOrder) throws IOException {
        super(in);
        this.service = service;
        this.byteOrder = byteOrder;
    }

    public ZstdObjectDataInput(ObjectDataInput in) throws IOException {
        this((InputStream) in, (InternalSerializationService) ((SerializationServiceSupport) in).getSerializationService(),
                in.getByteOrder());
        version = in.getVersion();
        wanProtocolVersion = in.getWanProtocolVersion();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, final int off, final int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        int read = 0;
        while (read < len) {
            int count = read(b, off + read, len - read);
            if (count < 0) {
                throw new EOFException("End of stream reached");
            }
            read += count;
        }
    }

    @Override
    public final boolean readBoolean() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    @Override
    public final byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    @Override
    public final short readShort() throws IOException {
        readFully(numberBuffer, 0, SHORT_SIZE_IN_BYTES);
        return Bits.readShort(numberBuffer, 0, bigEndian());
    }

    @Override
    public final char readChar() throws IOException {
        readFully(numberBuffer, 0, CHAR_SIZE_IN_BYTES);
        return Bits.readChar(numberBuffer, 0, bigEndian());
    }

    @Override
    public final int readInt() throws IOException {
        readFully(numberBuffer, 0, INT_SIZE_IN_BYTES);
        return Bits.readInt(numberBuffer, 0, bigEndian());
    }

    @Override
    public final long readLong() throws IOException {
        readFully(numberBuffer, 0, LONG_SIZE_IN_BYTES);
        return Bits.readLong(numberBuffer, 0, bigEndian());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int total = 0;
        int current = 0;

        while ((total < n) && ((current = (int) skip(n - total)) > 0)) {
            total += current;
        }

        return total;
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException {
        int numberOfBytes = readInt();
        if (numberOfBytes == NULL_ARRAY_LENGTH) {
            return null;
        }

        byte[] utf8Bytes = new byte[numberOfBytes];
        readFully(utf8Bytes);
        return new String(utf8Bytes, UTF_8);
    }

    @Override
    public byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];
    }

    @Override
    public boolean[] readBooleanArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            boolean[] values = new boolean[len];
            for (int i = 0; i < len; i++) {
                values[i] = readBoolean();
            }
            return values;
        }
        return new boolean[0];
    }

    @Override
    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            char[] values = new char[len];
            for (int i = 0; i < len; i++) {
                values[i] = readChar();
            }
            return values;
        }
        return new char[0];
    }

    @Override
    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                values[i] = readInt();
            }
            return values;
        }
        return new int[0];
    }

    @Override
    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                values[i] = readLong();
            }
            return values;
        }
        return new long[0];
    }

    @Override
    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            double[] values = new double[len];
            for (int i = 0; i < len; i++) {
                values[i] = readDouble();
            }
            return values;
        }
        return new double[0];
    }

    @Override
    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            float[] values = new float[len];
            for (int i = 0; i < len; i++) {
                values[i] = readFloat();
            }
            return values;
        }
        return new float[0];
    }

    @Override
    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            short[] values = new short[len];
            for (int i = 0; i < len; i++) {
                values[i] = readShort();
            }
            return values;
        }
        return new short[0];
    }

    @Override
    public String[] readUTFArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            String[] values = new String[len];
            for (int i = 0; i < len; i++) {
                values[i] = readUTF();
            }
            return values;
        }
        return new String[0];
    }

    @Override
    public <T> T readObject() throws IOException {
        return service.readObject(this);
    }

    @Override
    public <T> T readObject(Class aClass) throws IOException {
        return service.readObject(this, aClass);
    }

    @Override
    public Data readData() throws IOException {
        byte[] bytes = readByteArray();
        return bytes == null ? null : new HeapData(bytes);
    }

    @Override
    public <T> T readDataAsObject() throws IOException {
        Data data = readData();
        return data == null ? null : (T) service.toObject(data);
    }

    @Override
    public ClassLoader getClassLoader() {
        return service.getClassLoader();
    }


    @Override
    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    private boolean bigEndian() {
        return byteOrder == ByteOrder.BIG_ENDIAN;
    }

    /**
     * {@inheritDoc}
     * If the serializer supports versioning it may set the version to use for
     * the intra-cluster message serialization on this object.
     *
     * @return the version of {@link Version#UNKNOWN} if the version is unknown to the object.
     */
    @Override
    public Version getVersion() {
        return version;
    }

    /**
     * {@inheritDoc}
     * If the serializer supports versioning it may set the version to use for
     * the intra-cluster message serialization on this object.
     *
     * @param version version to set
     */
    @Override
    public void setVersion(Version version) {
        this.version = version;
    }

    @Override
    public void setWanProtocolVersion(Version version) {
        this.wanProtocolVersion = version;
    }

    @Override
    public Version getWanProtocolVersion() {
        return wanProtocolVersion;
    }
}
