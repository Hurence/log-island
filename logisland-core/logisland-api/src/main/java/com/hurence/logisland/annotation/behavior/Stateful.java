/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.annotation.behavior;

import com.hurence.logisland.processor.Processor;

import java.lang.annotation.*;

import com.hurence.logisland.processor.state.Scope;
import com.hurence.logisland.processor.state.StateManager;

/**
 * Annotation that may be placed on a
 * {@link Processor} indicating that this
 * processor maintain its state after a process call.
 *
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Stateful {
    /**
     * Provides a description of what information is being stored in the {@link StateManager}
     */
    String description();

    /**
     * Indicates the Scope(s) associated with the State that is stored and retrieved.
     */
    Scope[] scopes();

}
