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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.version.Version;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;

import java.io.IOException;
import java.io.OutputStream;
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
 */
public class LZ4BlockObjectDataOutput extends LZ4BlockOutputStream implements CompressionObjectDataOutput {

    private final InternalSerializationService service;
    private final ByteOrder byteOrder;
    private final byte[] numberBuffer = new byte[DOUBLE_SIZE_IN_BYTES];
    private Version version = Version.UNKNOWN;
    private Version wanProtocolVersion = Version.UNKNOWN;

    public LZ4BlockObjectDataOutput(OutputStream out, InternalSerializationService service, ByteOrder byteOrder) {
        super(out, 1 << 20, LZ4Factory.fastestJavaInstance().fastCompressor());
        this.service = service;
        this.byteOrder = byteOrder;
    }

    public LZ4BlockObjectDataOutput(ObjectDataOutput out) {
        this((OutputStream) out, (InternalSerializationService) ((SerializationServiceSupport) out).getSerializationService(),
                out.getByteOrder());
        version = out.getVersion();
        wanProtocolVersion = out.getWanProtocolVersion();
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        write(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        Bits.writeShort(numberBuffer, 0, (short) v, bigEndian());
        write(numberBuffer, 0, SHORT_SIZE_IN_BYTES);
    }

    @Override
    public void writeChar(int v) throws IOException {
        Bits.writeChar(numberBuffer, 0, (char) v, bigEndian());
        write(numberBuffer, 0, CHAR_SIZE_IN_BYTES);
    }

    @Override
    public void writeInt(int v) throws IOException {
        Bits.writeInt(numberBuffer, 0, v, bigEndian());
        write(numberBuffer, 0, INT_SIZE_IN_BYTES);
    }

    @Override
    public void writeLong(long v) throws IOException {
        Bits.writeLong(numberBuffer, 0, v, bigEndian());
        write(numberBuffer, 0, LONG_SIZE_IN_BYTES);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            write((byte) s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            writeChar(v);
        }
    }

    @Override
    public void writeByteArray(byte[] bytes) throws IOException {
        int len = (bytes != null) ? bytes.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            write(bytes);
        }
    }

    @Override
    public void writeBooleanArray(boolean[] booleans) throws IOException {
        int len = booleans != null ? booleans.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (boolean c : booleans) {
                writeBoolean(c);
            }
        }

    }

    @Override
    public void writeCharArray(char[] chars) throws IOException {
        int len = chars != null ? chars.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (char c : chars) {
                writeChar(c);
            }
        }
    }

    @Override
    public void writeIntArray(int[] ints) throws IOException {
        int len = ints != null ? ints.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (int i : ints) {
                writeInt(i);
            }
        }
    }

    @Override
    public void writeLongArray(long[] longs) throws IOException {
        int len = longs != null ? longs.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (long l : longs) {
                writeLong(l);
            }
        }
    }

    @Override
    public void writeDoubleArray(double[] doubles) throws IOException {
        int len = doubles != null ? doubles.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (double d : doubles) {
                writeDouble(d);
            }
        }
    }

    @Override
    public void writeFloatArray(float[] floats) throws IOException {
        int len = floats != null ? floats.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (float f : floats) {
                writeFloat(f);
            }
        }
    }

    @Override
    public void writeShortArray(short[] shorts) throws IOException {
        int len = shorts != null ? shorts.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (short s : shorts) {
                writeShort(s);
            }
        }
    }

    @Override
    public void writeUTFArray(String[] strings) throws IOException {
        int len = strings != null ? strings.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (String s : strings) {
                writeUTF(s);
            }
        }
    }

    @Override
    public void writeUTF(String str) throws IOException {
        if (str == null) {
            writeInt(NULL_ARRAY_LENGTH);
            return;
        }

        byte[] utf8Bytes = str.getBytes(UTF_8);
        writeInt(utf8Bytes.length);
        write(utf8Bytes);
    }

    @Override
    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    @Override
    public void writeData(Data data) throws IOException {
        byte[] payload = data != null ? data.toByteArray() : null;
        writeByteArray(payload);
    }

    @Override
    public byte[] toByteArray() {
        return toByteArray(0);
    }

    @Override
    public byte[] toByteArray(int padding) {
        throw new UnsupportedOperationException();
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
