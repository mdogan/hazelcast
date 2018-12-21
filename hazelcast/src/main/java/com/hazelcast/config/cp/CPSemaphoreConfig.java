/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.cp;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CPSemaphoreConfig extends AbstractCPObjectConfig {

    public static final boolean DEFAULT_SEMAPHORE_JDK_COMPATIBILITY = false;


    private boolean jdkCompatible = DEFAULT_SEMAPHORE_JDK_COMPATIBILITY;

    public CPSemaphoreConfig() {
        super();
    }

    public CPSemaphoreConfig(String name) {
        super(name);
    }

    public CPSemaphoreConfig(String name, boolean jdkCompatible) {
        super(name);
        this.jdkCompatible = jdkCompatible;
    }

    public CPSemaphoreConfig setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isJdkCompatible() {
        return jdkCompatible;
    }

    public CPSemaphoreConfig setJdkCompatible(boolean jdkCompatible) {
        this.jdkCompatible = jdkCompatible;
        return this;
    }
}
