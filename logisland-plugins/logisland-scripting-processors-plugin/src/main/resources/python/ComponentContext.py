#!/usr/bin/env python
""" generated source for module ComponentContext """
# 
#  * Copyright (C) 2016 Hurence
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *         http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  
# package: com.hurence.logisland.component
import com.hurence.logisland.component.PropertyDescriptor

import com.hurence.logisland.component.PropertyValue

import java.util.Map

# 
#  * <p>
#  * Provides a bridge between a Processor and the Framework
#  * </p>
#  *
#  * <p>
#  * <b>Note: </b>Implementations of this interface are NOT necessarily
#  * thread-safe.
#  * </p>
#  
class ComponentContext:
    """ generated source for interface ComponentContext """
    __metaclass__ = ABCMeta
    # 
    #      * Retrieves the current value set for the given descriptor, if a value is
    #      * set - else uses the descriptor to determine the appropriate default value
    #      *
    #      * @param descriptor to lookup the value of
    #      * @return the property value of the given descriptor
    #      
    @abstractmethod
    @overloaded
    def getProperty(self, descriptor):
        """ generated source for method getProperty """

    # 
    #      * Retrieves the current value set for the given descriptor, if a value is
    #      * set - else uses the descriptor to determine the appropriate default value
    #      *
    #      * @param propertyName of the property to lookup the value for
    #      * @return property value as retrieved by property name
    #      
    @abstractmethod
    @getProperty.register(object, str)
    def getProperty_0(self, propertyName):
        """ generated source for method getProperty_0 """

    # 
    #      * Creates and returns a {@link PropertyValue} object that can be used for
    #      * evaluating the value of the given String
    #      *
    #      * @param rawValue the raw input before any property evaluation has occurred
    #      * @return a {@link PropertyValue} object that can be used for
    #      * evaluating the value of the given String
    #      
    @abstractmethod
    def newPropertyValue(self, rawValue):
        """ generated source for method newPropertyValue """

    # 
    #      * @return a Map of all PropertyDescriptors to their configured getAllFields. This
    #      * Map may or may not be modifiable, but modifying its getAllFields will not
    #      * change the getAllFields of the processor's properties
    #      
    @abstractmethod
    def getProperties(self):
        """ generated source for method getProperties """

    # 
    #      * @return the configured name of this processor
    #      
    @abstractmethod
    def getName(self):
        """ generated source for method getName """

