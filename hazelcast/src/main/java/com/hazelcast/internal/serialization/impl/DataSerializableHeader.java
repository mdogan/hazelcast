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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.impl.compression.CompressionObjectDataInput;
import com.hazelcast.internal.serialization.impl.compression.CompressionObjectDataOutput;
import com.hazelcast.internal.serialization.impl.compression.GzipObjectDataInput;
import com.hazelcast.internal.serialization.impl.compression.GzipObjectDataOutput;
import com.hazelcast.internal.serialization.impl.compression.LZ4BlockObjectDataInput;
import com.hazelcast.internal.serialization.impl.compression.LZ4BlockObjectDataOutput;
import com.hazelcast.internal.serialization.impl.compression.ZstdObjectDataInput;
import com.hazelcast.internal.serialization.impl.compression.ZstdObjectDataOutput;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * The serializaion header consists of one java byte primitive value.
 * Bits are used in the following way (from the least significant to the most significant)
 * 0.) 0=data_serializable, 1=identified_data_serializable
 * 1.) 0=non-versioned, 1=versioned
 * 2.) 0=un-compressed, 1=compressed
 * 3.) unused
 * 4.) unused
 * 5.) unused
 * 6.) unused
 * 7.) unused
 */
final class DataSerializableHeader {

    static final byte IDS = 0;
    static final byte VERSIONED = 1;
    static final byte COMPRESSED = 2;

    private DataSerializableHeader() {
    }

    static boolean isIdentifiedDataSerializable(byte header) {
        return Bits.isBitSet(header, IDS);
    }

    static boolean isVersioned(byte header) {
        return Bits.isBitSet(header, VERSIONED);
    }

    static boolean isCompressed(byte header) {
        return Bits.isBitSet(header, COMPRESSED);
    }

    static byte createHeader(boolean identified, boolean versioned, boolean compressed) {
        byte header = 0;

        if (identified) {
            header = Bits.setBit(header, IDS);
        }
        if (versioned) {
            header = Bits.setBit(header, VERSIONED);
        }
        if (compressed) {
            header = Bits.setBit(header, COMPRESSED);
        }

        return header;
    }

    static final CompressionMethod COMPRESSION;

    enum CompressionMethod {
        NONE, ZSTD, GZIP, LZ4
    }

    static {
        String method = System.getProperty("hazelcast.serialization.compression.method", CompressionMethod.NONE.toString());
        COMPRESSION = CompressionMethod.valueOf(method);
        Logger.getLogger(DataSerializableHeader.class).severe("!!! ============== USING " + COMPRESSION + " COMPRESSION METHOD ============== !!!");
    }

    static CompressionObjectDataOutput createCompressionOutput(ObjectDataOutput out) throws IOException {
        switch (COMPRESSION) {
            case LZ4:
                if (!(out instanceof LZ4BlockObjectDataOutput)) {
                    assert !(out instanceof CompressionObjectDataOutput) : String.valueOf(out);
                    return new LZ4BlockObjectDataOutput(out);
                }
                break;
            case GZIP:
                if (!(out instanceof GzipObjectDataOutput)) {
                    assert !(out instanceof CompressionObjectDataOutput) : String.valueOf(out);
                    return new GzipObjectDataOutput(out);
                }
                break;
            case ZSTD:
                if (!(out instanceof ZstdObjectDataOutput)) {
                    assert !(out instanceof CompressionObjectDataOutput) : String.valueOf(out);
                    return new ZstdObjectDataOutput(out);
                }
                break;
        }
        return null;
    }

    static CompressionObjectDataInput createCompressionInput(ObjectDataInput in) throws IOException {
        switch (COMPRESSION) {
            case LZ4:
                if (!(in instanceof LZ4BlockObjectDataInput)) {
                    assert !(in instanceof CompressionObjectDataInput) : String.valueOf(in);
                    return new LZ4BlockObjectDataInput(in);
                }
                break;
            case GZIP:
                if (!(in instanceof GzipObjectDataInput)) {
                    assert !(in instanceof CompressionObjectDataInput) : String.valueOf(in);
                    return new GzipObjectDataInput(in);
                }
                break;
            case ZSTD:
                if (!(in instanceof ZstdObjectDataInput)) {
                    assert !(in instanceof CompressionObjectDataInput) : String.valueOf(in);
                    return new ZstdObjectDataInput(in);
                }
                break;
        }
        return null;
    }
}
