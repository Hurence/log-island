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
package com.hurence.logisland.rest.processor.lookup;

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.service.rest.RestClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHttpProcessor extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHttpProcessor.class);

    public static final PropertyDescriptor HTTP_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("http.client.service")
            .description("The instance of the Controller Service to use for HTTP requests.")
            .required(true)
            .identifiesControllerService(RestClientService.class)
            .build();


    protected RestClientService restClientService;

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        restClientService = PluginProxy.rewrap(context.getPropertyValue(HTTP_CLIENT_SERVICE).asControllerService());
        if (restClientService == null) {
            logger.error("Http Rest client service is not initialized!");
            throw new InitializationException("Could not initialize Http Rest client service!");
        }
    }


}