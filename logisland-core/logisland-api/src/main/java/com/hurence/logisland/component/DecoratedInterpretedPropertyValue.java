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
package com.hurence.logisland.component;

import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.expressionlanguage.InterpreterEngineException;
import com.hurence.logisland.processor.state.DataUnit;
import com.hurence.logisland.record.Record;

/**
 * That class encapsulates an InterpretedPropertyValue instance with an associated Record instance.
 */
class DecoratedInterpretedPropertyValue extends AbstractPropertyValue {

    public DecoratedInterpretedPropertyValue(InterpretedPropertyValue interpretedPropertyValue, Record record) {
        try {
            rawValue = interpretedPropertyValue.getRawValue(record);
        } catch (InterpreterEngineException iee) {
            throw new RuntimeException(iee);
        }
    }

    @Override
    public <T extends ControllerService> T asControllerService(Class<T> serviceType) throws IllegalArgumentException {
        return null;
    }

    @Override
    public Double asDataSize(DataUnit dataUnit) {
        return null;
    }
}
