/**
  * Copyright (C) 2016 Hurence (support@hurence.com)
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
/**
  * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.stream.spark.structured.provider

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.serializer.{NoopSerializer, RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.structured.provider.KafkaProperties._
import com.hurence.logisland.stream.spark.structured.provider.KafkaStructuredStreamProviderService._
import com.hurence.logisland.stream.spark.structured.provider.StructuredStreamProviderServiceWriter.OUTPUT_MODE
import com.hurence.logisland.util.kafka.KafkaSink
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Compatible with kafka 0.10.0 or higher
  */
@CapabilityDescription("Provide a ways to use kafka as input or output in StructuredStream streams")
class KafkaStructuredStreamProviderService() extends AbstractControllerService
  with StructuredStreamProviderServiceReader
  with StructuredStreamProviderServiceWriter {

  var kafkaSinkParams: Map[String, Object] = _
  var kafkaParams: Map[String, Object] = _
  // Define the Kafka parameters, broker list must be specified
  var inputTopics = Set[String]()
  var outputTopics = Set[String]()
  var topicAutocreate = true
  var topicDefaultPartitions = 3
  var topicDefaultReplicationFactor = 1
  var brokerList = ""
  var zkQuorum = ""
  var kafkaBatchSize = "16384"
  var kafkaLingerMs = "5"
  var kafkaAcks = "0"
  var kafkaOffset = "latest"

  var securityProtocol = ""
  var saslKbServiceName = ""
  var startingOffsets = ""
  var failOnDataLoss = true
  var maxOffsetsPerTrigger: Option[Long] = None

  var readValueSerializer: RecordSerializer = _
  var writeValueSerializer: RecordSerializer = _
  var writeKeySerializer: RecordSerializer = _
  var outputMode: String = _

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
    this.synchronized {
      try {

        // Define the Kafka parameters, broker list must be specified
        inputTopics = context.getPropertyValue(INPUT_TOPICS).asString.split(",").toSet
        outputTopics = context.getPropertyValue(OUTPUT_TOPICS).asString.split(",").toSet

        topicAutocreate = context.getPropertyValue(KAFKA_TOPIC_AUTOCREATE).asBoolean().booleanValue()
        topicDefaultPartitions = context.getPropertyValue(KAFKA_TOPIC_DEFAULT_PARTITIONS).asInteger().intValue()
        topicDefaultReplicationFactor = context.getPropertyValue(KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR).asInteger().intValue()
        brokerList = context.getPropertyValue(KAFKA_METADATA_BROKER_LIST).asString
        zkQuorum = context.getPropertyValue(KAFKA_ZOOKEEPER_QUORUM).asString

        securityProtocol = context.getPropertyValue(KAFKA_SECURITY_PROTOCOL).asString
        saslKbServiceName = context.getPropertyValue(KAFKA_SASL_KERBEROS_SERVICE_NAME).asString

        startingOffsets = context.getPropertyValue(KAFKA_STARTING_OFFSETS).asString
        failOnDataLoss = context.getPropertyValue(KAFKA_FAIL_ON_DATA_LOSS).asBoolean

        maxOffsetsPerTrigger = if (context.getPropertyValue(KAFKA_MAX_OFFSETS_PER_TRIGGER).isSet)
          Some(context.getPropertyValue(KAFKA_MAX_OFFSETS_PER_TRIGGER).asLong())
        else None

        // TODO deprecate topic creation here (must be done through the agent)
        if (topicAutocreate) {
          val zkUtils = ZkUtils.apply(zkQuorum, 10000, 10000, JaasUtils.isZkSecurityEnabled)
          createTopicsIfNeeded(zkUtils, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
          createTopicsIfNeeded(zkUtils, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
        }

        readValueSerializer = SerializerProvider.getSerializer(
          context.getPropertyValue(READ_VALUE_SERIALIZER).asString,
          context.getPropertyValue(AVRO_READ_VALUE_SCHEMA).asString)

        writeValueSerializer = SerializerProvider.getSerializer(
          context.getPropertyValue(WRITE_VALUE_SERIALIZER).asString,
          context.getPropertyValue(AVRO_WRITE_VALUE_SCHEMA).asString)

        writeKeySerializer = SerializerProvider.getSerializer(
          context.getPropertyValue(WRITE_KEY_SERIALIZER).asString, null)

        if (context.getPropertyValue(OUTPUT_MODE).isSet) {
          outputMode = context.getPropertyValue(OUTPUT_MODE).asString()
        }
      } catch {
        case e: Exception =>
          throw new InitializationException(e)
      }
    }
  }

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param spark
    * @return DataFrame currently loaded
    */
  override def read(spark: SparkSession) = {
    import spark.implicits._
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    getLogger.info(s"starting Kafka direct stream on topics $inputTopics from $startingOffsets offsets, and maxOffsetsPerTrigger :$maxOffsetsPerTrigger")
    var df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("kafka.security.protocol", securityProtocol)
      .option("kafka.sasl.kerberos.service.name", saslKbServiceName)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", failOnDataLoss)
      .option("subscribe", inputTopics.mkString(","))



    if (maxOffsetsPerTrigger.isDefined) {
      df = df.option("maxOffsetsPerTrigger", maxOffsetsPerTrigger.get)
    }

    df.load()
      .selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)")
      .as[(Array[Byte], Array[Byte])]
      .flatMap(r => {
        readValueSerializer match {
          case sr: NoopSerializer => Some(new StandardRecord("kafka")
            .setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, r._1)
            .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, r._2))
          case _ => SerializingTool.deserializeRecords(readValueSerializer, r._2)
        }
      })
  }

  /**
    * Allows subclasses to register which property descriptor objects are
    * supported.
    *
    * @return PropertyDescriptor objects this processor currently supports
    */
  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.add(INPUT_TOPICS)
    descriptors.add(OUTPUT_TOPICS)
    descriptors.add(KAFKA_TOPIC_AUTOCREATE)
    descriptors.add(KAFKA_TOPIC_DEFAULT_PARTITIONS)
    descriptors.add(KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR)
    descriptors.add(KAFKA_METADATA_BROKER_LIST)
    descriptors.add(KAFKA_ZOOKEEPER_QUORUM)
    descriptors.add(KAFKA_STARTING_OFFSETS)
    descriptors.add(KAFKA_FAIL_ON_DATA_LOSS)
    descriptors.add(KAFKA_MAX_OFFSETS_PER_TRIGGER)
    descriptors.add(KAFKA_SECURITY_PROTOCOL)
    descriptors.add(KAFKA_SASL_KERBEROS_SERVICE_NAME)
    descriptors.add(READ_VALUE_SERIALIZER)
    descriptors.add(AVRO_READ_VALUE_SCHEMA)
    descriptors.add(WRITE_VALUE_SERIALIZER)
    descriptors.add(AVRO_WRITE_VALUE_SCHEMA)
    descriptors.add(WRITE_KEY_SERIALIZER)
    descriptors.add(OUTPUT_MODE)
    Collections.unmodifiableList(descriptors)
  }

  /**
    * Topic creation
    *
    * @param zkUtils
    * @param topics
    * @param topicDefaultPartitions
    * @param topicDefaultReplicationFactor
    */
  def createTopicsIfNeeded(zkUtils: ZkUtils,
                           topics: Set[String],
                           topicDefaultPartitions: Int,
                           topicDefaultReplicationFactor: Int): Unit = {

    topics.foreach(topic => {

      if (!topic.equals(NONE_TOPIC) && !AdminUtils.topicExists(zkUtils, topic)) {
        AdminUtils.createTopic(zkUtils, topic, topicDefaultPartitions, topicDefaultReplicationFactor)
        Thread.sleep(1000)
        getLogger.info(s"created topic $topic with" +
          s" $topicDefaultPartitions partitions and" +
          s" $topicDefaultReplicationFactor replicas")
      }
    })
  }

  /**
    * create a streaming DataFrame that represents data received
    *
    * @return DataFrame currently loaded
    */
  override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]) = {
    val sender = df.sparkSession.sparkContext.broadcast(KafkaSink(kafkaSinkParams))

    import df.sparkSession.implicits._

    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    val dataStreamWriter =  df
      .mapPartitions(record => record.map(record => SerializingTool.serializeRecords(writeValueSerializer, writeKeySerializer, record)))
      // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
      .map(r => {
        (r.getField(FieldDictionary.RECORD_KEY).asBytes(), r.getField(FieldDictionary.RECORD_VALUE).asBytes())
      })
      .as[(Array[Byte], Array[Byte])]
      .toDF("key", "value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerList)
      .option("kafka.security.protocol", securityProtocol)
      .option("kafka.sasl.kerberos.service.name", saslKbServiceName)
      .option("topic", outputTopics.mkString(","))
      .option("checkpointLocation", "checkpoints") //Rewind not working because of this ? TODO make this configurable check where to put it, source ? writer ? both ?

    if (outputMode != null) {
      dataStreamWriter.outputMode(outputMode)
    }

    dataStreamWriter
      .queryName(getIdentifier + "_sink")//MUST be unique !! so we can not use the same sink several times... TODO move this into stream so he can make a unique identifier
      .start()
  }
}

object KafkaStructuredStreamProviderService {
  val READ_VALUE_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.value.serializer")
    .description("the serializer to use to deserialize value of topic messages as record")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val AVRO_READ_VALUE_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.value.schema")
    .description("the avro schema definition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val WRITE_VALUE_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.value.serializer")
    .description("the serializer to use to serialize records into value topic messages")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val AVRO_WRITE_VALUE_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.value.schema")
    .description("the avro schema definition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val WRITE_KEY_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.key.serializer")
    .description("The key serializer to use")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build
}
