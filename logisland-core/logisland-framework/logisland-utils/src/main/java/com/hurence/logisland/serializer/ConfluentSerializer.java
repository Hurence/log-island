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

package com.hurence.logisland.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class ConfluentSerializer implements RecordSerializer {

  private Logger logger = LoggerFactory.getLogger(ConfluentSerializer.class);
  protected String registryUrl;
  protected String topicField = "topic";

  protected transient SchemaRegistryClient schemaRegistryClient;
  protected transient KafkaAvroDeserializer deserializer;
  protected transient KafkaAvroSerializer serializer;

  public ConfluentSerializer(String schemaInfo) {
    try {
      JSONObject obj = new JSONObject(schemaInfo);
      this.registryUrl = obj.getString("registryUrl");
      if (obj.has("topicField")) {
        this.topicField = obj.getString("topicField");
      }
    } catch (Exception e) {
      logger.error("Error parsing ConfluentSerializer configuration", e);
      throw new RuntimeException(e);
    }
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    if (schemaRegistryClient == null) {
      Properties defaultConfig = new Properties();
      defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.registryUrl);
      defaultConfig.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT);
      schemaRegistryClient = new CachedSchemaRegistryClient(new RestService(registryUrl), 10, new HashMap(defaultConfig));
    }
    return schemaRegistryClient;
  }

  public KafkaAvroSerializer getSerializer() {
    if (serializer == null) {
      Properties defaultConfig = new Properties();
      defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.registryUrl);
      defaultConfig.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_DEFAULT);
      serializer = new KafkaAvroSerializer(getSchemaRegistryClient(), new HashMap(defaultConfig));
    }
    return serializer;
  }

  public KafkaAvroDeserializer getDeserializer() {
    if (deserializer == null) {
      deserializer = new KafkaAvroDeserializer(getSchemaRegistryClient());
    }
    return deserializer;
  }

  // for unit test purposes
  public ConfluentSerializer(MockSchemaRegistryClient schemaRegistryClientMock, final InputStream inputStream) {
    assert inputStream != null;
    final Schema.Parser parser = new Schema.Parser();
    try {
      schemaRegistryClient = schemaRegistryClientMock;
      registryUrl = "bogus";
      Schema schema = parser.parse(inputStream);
      Optional<ParsedSchema> parsedSchema = schemaRegistryClient.parseSchema(null, schema.toString(true), new LinkedList<SchemaReference>());
      getSchemaRegistryClient().register("logisland_events", parsedSchema.get());
    } catch (Exception e) {
      logger.error("Error initalizing schema registry", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void serialize(OutputStream objectDataOutput, Record record) throws RecordSerializationException {
    if (record.hasField(topicField)) {
      String topic = record.getField(topicField).asString();
      final Schema.Parser parser = new Schema.Parser();
      try {
        Schema schema = parser.parse(getSchemaRegistryClient().getLatestSchemaMetadata(topic).getSchema());
        GenericRecord eventRecord = writeAvro(record, schema);
        byte[] bytes = getSerializer().serialize(topic, eventRecord);
        logger.trace(Arrays.toString(bytes));
        logger.trace(getSchemaId(bytes).toString());
        objectDataOutput.write(bytes);
      } catch (SerializationException e1) {
        e1.printStackTrace();
      } catch (IOException e1) {
        e1.printStackTrace();
      } catch (RestClientException e1) {
        e1.printStackTrace();
      } catch (Exception e1) {
        logger.error(e1.getStackTrace().toString());
        e1.printStackTrace();
      }
    } else {
      throw new SerializationException("The serialize method has not been implemented since unknown topic");
    }
  }

  @Override
  public Record deserialize(InputStream objectDataInput) throws RecordSerializationException {
    byte[] bytes = new byte[0];
    try {
      bytes = IOUtils.toByteArray(objectDataInput);
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.trace(Arrays.toString(bytes));
    logger.trace(getSchemaId(bytes).toString());
    GenericRecord genericRecord = (GenericRecord) getDeserializer().deserialize(null, bytes);
    Record record = new StandardRecord("record");
    readAvro(record, genericRecord);
    logger.trace(record.toString());
    return record;
  }

  /** 
   * Get schemaId from bytes[] when serialize and deserialize
   */
  private Integer getSchemaId(byte[] bytes) {
    byte[] schemaId = new byte[4];
    System.arraycopy(bytes, 1, schemaId, 0, 4);
    return new Integer(ByteBuffer.wrap(schemaId).getInt());
  }

  /**
   * convert the logIsland Event to an Avro GenericRecord
   */
  private GenericRecord writeAvro(final Record record, Schema schema) {
    GenericRecord eventRecord = new GenericData.Record(schema);
    for (Map.Entry<String, Field> entry : record.getFieldsEntrySet()) {
      // retrieve event field
      String key = entry.getKey();
      Field field = entry.getValue();
      Object value = field.getRawValue();

      // dump event field as record attribute
      eventRecord.put(key, value);
    }
    return eventRecord;
  }

  /**
   * Converts the AVRO GenericRecord to a logisland record.
   *
   * @param record        the logisland record to fill.
   * @param genericRecord the AVRO record to convert.
   * 
   * @return a logisland record built from the specified AVRO record.
   */
  private Record readAvro(final Record record, final GenericRecord genericRecord) {
    return readAvro(record, new AvroRecord() {
      @Override
      public Schema getSchema() {
        return genericRecord.getSchema();
      }

      @Override
      public Object get(String fieldName) {
        return genericRecord.get(fieldName);
      }
    });
  }

  private Record readAvro(final GenericData.Record avroRecord) {
    return readAvro(new StandardRecord(avroRecord.getSchema().getName()), new AvroRecord() {
      @Override
      public Schema getSchema() {
        return avroRecord.getSchema();
      }

      @Override
      public Object get(String fieldName) {
        return avroRecord.get(fieldName);
      }
    });
  }

  // This interface provides similar accesses to GenericData.Record and GenericRecord.
  private interface AvroRecord {
    Schema getSchema();

    Object get(String fieldName);
  }

  /**
   * Returns {@code null} if the provided value is a reference to
   * JsonProperties.NULL_VALUE; the same value is returned otherwise.
   *
   * @param fieldValue the value to check.
   *
   * @return {@code null} if the provided value is a reference to
   * JsonProperties.NULL_VALUE; the same value otherwise.
   */
  private Object handleJsonNull(Object fieldValue) {
    if (fieldValue == JsonProperties.NULL_VALUE) {
      fieldValue = null;
    }
    return fieldValue;
  }

  /**
   * Converts the AVRO record to a logisland record.
   *
   * @param record the logisland record to fill.
   * @param avroRecord the AVRO record to convert.
   *
   * @return a logisland record built from the specified AVRO record.
   */
  private Record readAvro(final Record record, final AvroRecord avroRecord) {
    final Schema avroSchema = avroRecord.getSchema();
    for (final Schema.Field schemaField : avroSchema.getFields()) {
      final String fieldName = schemaField.name();

      Object fieldValue = avroRecord.get(fieldName);
      if (fieldValue == null) {
        fieldValue = schemaField.defaultVal();
      }
      fieldValue = handleJsonNull(fieldValue);

      Schema.Type type = schemaField.schema().getType();

      if (type == Schema.Type.UNION) {
        // Handle optional value
        for (final Schema subSchema : schemaField.schema().getTypes()) {
          if (subSchema.getType() == Schema.Type.NULL) {
            continue;
          }
          type = subSchema.getType();
        }
      }

      switch (type) {
      case ARRAY:
        record.setField(fieldName, FieldType.ARRAY,
            fieldValue != null ? readAvro((GenericData.Array) fieldValue) : null);
        break;

      case RECORD:
        record.setField(fieldName, FieldType.RECORD,
            fieldValue != null ? readAvro((GenericData.Record) fieldValue) : null);
        break;

      case BOOLEAN:
        // Prevent simple type with the null value
        if (fieldValue != null) {
          record.setField(fieldName, FieldType.BOOLEAN, fieldValue);
        }
        break;

      case BYTES:
        // Prevent simple type with the null value
        if (fieldValue != null) {
          record.setField(fieldName, FieldType.BYTES, fieldValue);
        }
        break;

      case DOUBLE:
        // Prevent simple type with the null value
        if (fieldValue != null) {
          record.setField(fieldName, FieldType.DOUBLE, fieldValue);
        }
        break;

      case FLOAT:
        // Prevent simple type with the null value
        if (fieldValue != null) {
          record.setField(fieldName, FieldType.FLOAT, fieldValue);
        }
        break;

      case INT:
        // Prevent simple type with the null value
        if (fieldValue != null) {
          record.setField(fieldName, FieldType.INT, fieldValue);
        }
        break;

      case LONG:
        // Prevent simple type with the null value
        if (fieldValue != null) {
          record.setField(fieldName, FieldType.LONG, fieldValue);
        }
        break;

      case STRING:
        record.setField(fieldName, FieldType.STRING, fieldValue != null ? fieldValue.toString() : null);
        break;

      case NULL:
        record.setField(fieldName, FieldType.STRING, null);
        break;

      case ENUM:
        break;
      case FIXED:
        break;
      case MAP:
        break;
      case UNION:
        break;
      default:
        throw new UnsupportedOperationException("No support for AVRO type " + type);

      }
    }

    return record;
  }

  /**
   * Returns a List instance of objects filled from the provided AVRO array.
   *
   * @param avroArray the AVRO array that contains the values to convert.
   *
   * @return a List instance of objects filled from the provided AVRO array.
   */
  private List readAvro(final GenericData.Array avroArray) {
    final Schema.Type type = avroArray.getSchema().getElementType().getType();

    final List result = new ArrayList(avroArray.size());

    for (Object item : avroArray) {
      item = handleJsonNull(item);
      Object value = null;
      switch (type) {
      case ARRAY:
        value = item != null ? readAvro((GenericData.Array) item) : null;
        break;

      case RECORD:
        value = item != null ? readAvro((GenericData.Record) item) : null;
        break;

      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
        // Prevent simple type with the null value
        if (item == null) {
          continue;
        }
        break;

      case STRING:
        value = item != null ? item.toString() : null;
        break;

      case NULL:
        value = null;
        break;

      case ENUM:
      case FIXED:
      case MAP:
      case UNION:

      default:
        throw new UnsupportedOperationException("No support for AVRO type " + type);
      }
      result.add(value);
    }

    return result;
  }

}
