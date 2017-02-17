/**
 * Copyright (C) 2017 Hurence
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.bro;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.string.JsonUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bro (https://www.bro.org/) processor
 */
@Tags({"bro"})
@CapabilityDescription(
        "The Bro (https://www.bro.org) processor is the Logisland entry point to get and process Bro events."
        + " The Bro-Kafka plugin (https://github.com/bro/bro-plugins/tree/master/kafka ) should be used and configured"
        + " in order to have Bro events sent to Kafka  Then this Bro processor will do some minor pre-processing on"
        + " incoming Bro events to adapt them to Logisland. Then following processor in the stream can then process the"
        + " Bro events generated by this Bro processor.")
public class BroProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(BroProcessor.class);
    
    private Map<String, String> broFieldToLogislandField = new HashMap<String, String>();
    
    // Record field name for the destination ES index type
    private static final String ES_TYPE_FIELD = "record_es_type"; 
    
    // JSON Field name for the bro event type
    private static final String BRO_EVENT_TYPE = "bro_event_type";
    
    // Bro conn fields
    private static final String BRO_CONN_ID_ORIG_H = "id.orig_h";
    private static final String BRO_CONN_ID_RESP_H = "id.resp_h";
    private static final String BRO_CONN_ID_ORIG_P = "id.orig_p";
    private static final String BRO_CONN_ID_RESP_P = "id.resp_p";
    
    // Logisland conn fields
    
    private static final String LOGISLAND_CONN_SOURCE_IP = "source_ip";
    private static final String LOGISLAND_CONN_DEST_IP = "dest_ip";
    private static final String LOGISLAND_CONN_SOURCE_PORT = "source_port";
    private static final String LOGISLAND_CONN_DEST_PORT = "dest_port";

    @Override
    public void init(final ProcessContext context)
    {
        logger.debug("Initializing Bro Processor");
        
        // TODO add a config property to allow changing thids default bro fields mapping

        // Bro conn fields mapping
        broFieldToLogislandField.put(BRO_CONN_ID_ORIG_H, LOGISLAND_CONN_SOURCE_IP);
        broFieldToLogislandField.put(BRO_CONN_ID_RESP_H, LOGISLAND_CONN_DEST_IP);
        broFieldToLogislandField.put(BRO_CONN_ID_ORIG_P, LOGISLAND_CONN_SOURCE_PORT);
        broFieldToLogislandField.put(BRO_CONN_ID_RESP_P, LOGISLAND_CONN_DEST_PORT);
        
        logger.debug("################################### " + broFieldToLogislandField);
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        // TODO Auto-generated method stub
        return null;
    }
  
    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        logger.debug("Bro Processor records " + records);

        /**
         * Get the original Bro event as a JSON string and do some adaptation like
         * changing some field names:
         * - Bro field names with '.' are not acceptable for indexing into ES. Change them with something more
         * user friendly if needed but in any case, replace the ES unwanted characters like '.'.
         */
        for (Record record : records)
        {
            String recordValue = (String)record.getField(FieldDictionary.RECORD_VALUE).getRawValue();
            
            // Parse as JSON object
            Map<String, Object> jsonBroEvent = JsonUtil.convertJsonToMap(recordValue);

            if (jsonBroEvent.isEmpty())
            {
                logger.error("Empty Bro event or error while parsing it: " + record);
                continue;
            }
            
            if (jsonBroEvent.size() != 1)
            {
                logger.error("Bro event should have one bro event type field: " + record);
                continue;
            }
            
            Map.Entry<String, Object> eventTypeAndValue = jsonBroEvent.entrySet().iterator().next();
            
            String broEventType = eventTypeAndValue.getKey();
            Object broEventValue = eventTypeAndValue.getValue();
            
            Map<String, Object> finalBroEvent = null; 
            try {
                finalBroEvent = (Map<String, Object>)broEventValue;
            } catch(Throwable t)
            {
                logger.error("Cannot understand bro event content: " + record);
                continue;
            }
            
            finalBroEvent = replaceKeys(finalBroEvent, broFieldToLogislandField);
            
            // Add bro event type in event payload
            finalBroEvent.put(BRO_EVENT_TYPE, broEventType);
            
            String newRecordValue = JsonUtil.convertToJson(finalBroEvent);
             
            logger.debug("newRecordValue: " + newRecordValue);
            record.setStringField(FieldDictionary.RECORD_VALUE, newRecordValue);
            // Add special record field to indicate to ES processor which index type to use (index type is the bro event type)
            record.setStringField(ES_TYPE_FIELD, broEventType);
            
            logger.debug("new Bro record: " + record);
        }

        logger.debug("################## Bro Processor output records " + records);
        return records;
    }
    
    /**
     * Given a key mapping, replaces a key with another one, in a json object
     * @param json
     * @param oldToNewKey
     */
    private static Map<String, Object> replaceKeys(Map<String, Object> json, Map<String, String> oldToNewKeys)
    {        
        // Now replace any forbidden character in a key name. For the moment only the '.' character is identified as
        // a forbidden key name (for ES indexing)
        Map<String, Object> newJson = new HashMap<String, Object>();
        for (Map.Entry<String, Object> jsonEntry : json.entrySet())
        {
            String key = jsonEntry.getKey();
            Object value = jsonEntry.getValue();
            // Is it a key to replace ?
            if (oldToNewKeys.containsKey(key))
            {
                String newKey = oldToNewKeys.get(key);
                logger.debug("Replacing special " + key + " key with " + newKey);
                newJson.put(newKey, value);
            } else
            {
                // Not a special key to replace but we must at least remove unwanted characters
                String newKey = key.replaceAll("\\.", "_");
                newJson.put(newKey, value);
            }
        }
        
        return newJson;
    }
    
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        logger.debug("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);              
    }   
}
