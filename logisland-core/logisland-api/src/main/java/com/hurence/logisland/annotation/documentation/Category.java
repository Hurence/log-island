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
package com.hurence.logisland.annotation.documentation;

import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.processor.Processor;

import java.lang.annotation.*;

/**
 * Annotation that can be applied to a {@link Processor} or {@link ControllerService} in order to
 * associate category keyword with the component. This annotation do not affect the
 * component in any way but serve as additional documentation and can be used to
 * sort/filter Processors/Services.
 *
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Category {

    String value();
}
