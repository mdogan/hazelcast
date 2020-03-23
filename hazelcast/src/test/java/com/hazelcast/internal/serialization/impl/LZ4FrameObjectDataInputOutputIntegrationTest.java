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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compression.LZ4FrameObjectDataInput;
import com.hazelcast.internal.serialization.impl.compression.LZ4FrameObjectDataOutput;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LZ4FrameObjectDataInputOutputIntegrationTest
        extends AbstractDataStreamIntegrationTest<LZ4FrameObjectDataOutput, LZ4FrameObjectDataInput> {

    private ByteArrayOutputStream output;

    @Override
    protected byte[] getWrittenBytes() {
        return output.toByteArray();
    }

    @Override
    protected LZ4FrameObjectDataOutput getDataOutput(InternalSerializationService serializationService) {
        output = new ByteArrayOutputStream();
        try {
            return new LZ4FrameObjectDataOutput(output, serializationService, byteOrder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected LZ4FrameObjectDataInput getDataInputFromOutput() {
        try {
            out.flush();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        try {
            return new LZ4FrameObjectDataInput(new ByteArrayInputStream(getWrittenBytes()), serializationService, byteOrder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
