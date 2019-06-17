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
package com.hurence.logisland.processor.hbase;


import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.service.hbase.put.PutColumn;
import com.hurence.logisland.service.hbase.put.PutRecord;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.RecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Category(ComponentCategory.DATASTORE)
@Tags({"hadoop", "hbase"})
@CapabilityDescription("Adds the Contents of a Record to HBase as the value of a single cell")
@ExtraDetailFile("./details/PutHBaseCell-Detail.rst")
public class PutHBaseCell extends AbstractPutHBase {

    private static Logger logger = LoggerFactory.getLogger(PutHBaseCell.class);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME_FIELD);
        properties.add(ROW_ID_FIELD);
        properties.add(ROW_ID_ENCODING_STRATEGY);
        properties.add(COLUMN_FAMILY_FIELD);
        properties.add(COLUMN_QUALIFIER_FIELD);
        properties.add(BATCH_SIZE);
        properties.add(RECORD_SCHEMA);
        properties.add(RECORD_SERIALIZER);
        properties.add(TABLE_NAME_DEFAULT);
        properties.add(COLUMN_FAMILY_DEFAULT);
        properties.add(COLUMN_QUALIFIER_DEFAULT);
        return properties;
    }


    @Override
    protected PutRecord createPut(final ProcessContext context, final Record record, final RecordSerializer serializer) {

        String tableName = context.getPropertyValue(TABLE_NAME_DEFAULT).asString();
        String columnFamily = context.getPropertyValue(COLUMN_FAMILY_DEFAULT).asString();
        String columnQualifier = context.getPropertyValue(COLUMN_QUALIFIER_DEFAULT).asString();

        try {
            if (!record.hasField(context.getPropertyValue(ROW_ID_FIELD).asString()))
                throw new IllegalArgumentException("record has no ROW_ID_FIELD");

            final String row = record.getField(context.getPropertyValue(ROW_ID_FIELD).asString()).asString();

            if (record.hasField(context.getPropertyValue(TABLE_NAME_FIELD).asString()))
                tableName = record.getField(context.getPropertyValue(TABLE_NAME_FIELD).asString()).asString();

            if (record.hasField(context.getPropertyValue(COLUMN_FAMILY_FIELD).asString()))
                columnFamily = record.getField(context.getPropertyValue(COLUMN_FAMILY_FIELD).asString()).asString();

            if (record.hasField(context.getPropertyValue(COLUMN_QUALIFIER_FIELD).asString()))
                columnQualifier = record.getField(context.getPropertyValue(COLUMN_QUALIFIER_FIELD).asString()).asString();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializer.serialize(baos, record);
            final byte[] buffer = baos.toByteArray();
            baos.close();
            final Collection<PutColumn> columns = Collections.singletonList(new PutColumn(
                    columnFamily.getBytes(StandardCharsets.UTF_8),
                    columnQualifier.getBytes(StandardCharsets.UTF_8),
                    buffer));
            byte[] rowKeyBytes = getRow(row, context.getPropertyValue(ROW_ID_ENCODING_STRATEGY).asString());

            return new PutRecord(tableName, rowKeyBytes, columns, record);

        } catch (Exception e) {
            logger.error(e.toString());
        }


        return new PutRecord(tableName, null, Collections.emptyList(), record);
    }


}
