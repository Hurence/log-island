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
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"type", "fields", "update", "convert"})
@CapabilityDescription("Converts a field value into the given type. does nothing if conversion is not possible")
@DynamicProperty(name = "field",
        supportsExpressionLanguage = true,
        value = "the new type",
        description = "convert field value into new type")
@ExtraDetailFile("./details/common-processors/ConvertFieldsType-Detail.rst")
public class ConvertFieldsType extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ConvertFieldsType.class);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    private Map<String, FieldType> fieldTypes = null;

    @Override
    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        fieldTypes = getStringFieldTypes(context);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {


        for (Record record : records) {
            fieldTypes.keySet().forEach(fieldName -> {
                if (record.hasField(fieldName)) {

                    try {
                        Field currentField = record.getField(fieldName);
                        FieldType currentFieldType = currentField.getType();
                        FieldType newFieldType = fieldTypes.get(fieldName);

                        if(currentFieldType != newFieldType) {

                            switch (newFieldType) {

                                case STRING:
                                    record.setField(fieldName, newFieldType, currentField.asString());
                                    break;
                                case INT:
                                    record.setField(fieldName, newFieldType, currentField.asInteger());
                                    break;
                                case LONG:
                                    record.setField(fieldName, newFieldType, currentField.asLong());
                                    break;
                                case FLOAT:
                                    record.setField(fieldName, newFieldType, currentField.asFloat());
                                    break;
                                case DOUBLE:
                                    record.setField(fieldName, newFieldType, currentField.asDouble());
                                    break;
                                case BOOLEAN:
                                    record.setField(fieldName, newFieldType, currentField.asBoolean());
                                    break;
                                default:
                                    logger.info("field type {} is not supported yet", newFieldType.toString());
                                    break;
                            }
                        }
                    } catch (Throwable ex) {
                        logger.debug("unable to process a field in record : {}, {}", record, ex.getMessage());
                    }
                }
            });
        }

        return records;
    }

    private Map<String, FieldType> getStringFieldTypes(ProcessContext context) {
        Map<String, FieldType> fieldTypes = new HashMap<>();

        // loop over dynamic properties to add field types
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String field = entry.getKey().getName();
            final String type = entry.getValue().toLowerCase();
            FieldType fieldType = FieldType.STRING;
            switch (type) {

                case "string":
                    fieldType = FieldType.STRING;
                    break;
                case "int":
                case "integer":
                    fieldType = FieldType.INT;
                    break;
                case "long":
                    fieldType = FieldType.LONG;
                    break;
                case "float":
                    fieldType = FieldType.FLOAT;
                    break;
                case "double":
                    fieldType = FieldType.DOUBLE;
                    break;
                case "bool":
                case "boolean":
                    fieldType = FieldType.BOOLEAN;
                    break;
                default:
                    logger.debug("field type {} is not supported yet", type);
                    break;
            }
            fieldTypes.put(field, fieldType);
        }
        return fieldTypes;
    }


}
