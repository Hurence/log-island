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
/**
  * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.stream.spark

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.serializer._
import com.hurence.logisland.stream.{AbstractRecordStream, StreamContext}
import com.hurence.logisland.util.kafka.KafkaSink
import com.hurence.logisland.util.spark.{ControllerServiceLookupSink, SparkUtils, ZookeeperSink}
import com.hurence.logisland.validator.StandardValidators
import kafka.admin.AdminUtils
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory


object AbstractKafkaRecordStream {

    val DEFAULT_RAW_TOPIC = new AllowableValue("_raw", "default raw topic", "the incoming non structured topic")
    val DEFAULT_EVENTS_TOPIC = new AllowableValue("_events", "default events topic", "the outgoing structured topic")
    val DEFAULT_ERRORS_TOPIC = new AllowableValue("_errors", "default raw topic", "the outgoing structured error topic")

    val INPUT_TOPICS = new PropertyDescriptor.Builder()
        .name("kafka.input.topics")
        .description("Sets the input Kafka topic name")
        .required(true)
        .defaultValue(DEFAULT_RAW_TOPIC.getValue)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val OUTPUT_TOPICS = new PropertyDescriptor.Builder()
        .name("kafka.output.topics")
        .description("Sets the output Kafka topic name")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(DEFAULT_EVENTS_TOPIC.getValue)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val ERROR_TOPICS = new PropertyDescriptor.Builder()
        .name("kafka.error.topics")
        .description("Sets the error topics Kafka topic name")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(DEFAULT_ERRORS_TOPIC.getValue)
        .build

    val AVRO_INPUT_SCHEMA = new PropertyDescriptor.Builder()
        .name("avro.input.schema")
        .description("the avro schema definition")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val AVRO_OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
        .name("avro.output.schema")
        .description("the avro schema definition for the output serialization")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val AVRO_SERIALIZER = new AllowableValue(classOf[AvroSerializer].getName,
        "avro serialization", "serialize events as avro blocs")
    val JSON_SERIALIZER = new AllowableValue(classOf[JsonSerializer].getName,
        "json serialization", "serialize events as json blocs")
    val EXTENDED_JSON_SERIALIZER = new AllowableValue(classOf[ExtendedJsonSerializer].getName,
        "extended json serialization", "serialize events as json blocs supporting nested objects/arrays")
    val KRYO_SERIALIZER = new AllowableValue(classOf[KryoSerializer].getName,
        "kryo serialization", "serialize events as binary blocs")
    val STRING_SERIALIZER = new AllowableValue(classOf[StringSerializer].getName,
        "string serialization", "serialize events as string")
    val BYTESARRAY_SERIALIZER = new AllowableValue(classOf[BytesArraySerializer].getName,
        "byte array serialization", "serialize events as byte arrays")
    val KURA_PROTOCOL_BUFFER_SERIALIZER = new AllowableValue(classOf[KuraProtobufSerializer].getName,
        "Kura Protobuf serialization", "serialize events as Kura protocol buffer")
    val NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes")

    val INPUT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.input.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER)
        .defaultValue(KRYO_SERIALIZER.getValue)
        .build

    val OUTPUT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.output.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER)
        .defaultValue(KRYO_SERIALIZER.getValue)
        .build

    val ERROR_SERIALIZER = new PropertyDescriptor.Builder()
        .name("kafka.error.topics.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue(JSON_SERIALIZER.getValue)
      .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER)
        .build


    val KAFKA_TOPIC_AUTOCREATE = new PropertyDescriptor.Builder()
        .name("kafka.topic.autoCreate")
        .description("define wether a topic should be created automatically if not already exists")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build

    val KAFKA_TOPIC_DEFAULT_PARTITIONS = new PropertyDescriptor.Builder()
        .name("kafka.topic.default.partitions")
        .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("20")
        .build

    val KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR = new PropertyDescriptor.Builder()
        .name("kafka.topic.default.replicationFactor")
        .description("if autoCreate is set to true, this will set the number of replica for each partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("3")
        .build

    val KAFKA_METADATA_BROKER_LIST = new PropertyDescriptor.Builder()
        .name("kafka.metadata.broker.list")
        .description("a comma separated list of host:port brokers")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("sandbox:9092")
        .build

    val KAFKA_ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
        .name("kafka.zookeeper.quorum")
        .description("")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("sandbox:2181")
        .build

    val LARGEST_OFFSET = new AllowableValue("largest", "largest offset", "the offset to the largest offset")
    val SMALLEST_OFFSET = new AllowableValue("smallest", "smallest offset", "the offset to the smallest offset")

    val KAFKA_MANUAL_OFFSET_RESET = new PropertyDescriptor.Builder()
        .name("kafka.manual.offset.reset")
        .description("Sets manually an initial offset in ZooKeeper: "
            + "smallest (automatically reset the offset to the smallest offset), "
            + "largest (automatically reset the offset to the largest offset), "
            + "anything else (throw exception to the consumer)")
        .required(false)
        .allowableValues(LARGEST_OFFSET, SMALLEST_OFFSET)
        .build


    val KAFKA_MESSAGE_KEY_FIELD = new PropertyDescriptor.Builder()
        .name("kafka.message.key.field")
        .description("Sets the field which contains the key of the message " +
            "which will be sent to the output topic. " +
            "The default key field is set to record_id" +
            "If the field doesn't exist or is empty" +
            "the message key will be null and the partitioner will send the message" +
            "to a random partition, else the DefaultPartitioner will be used " +
            "to send the message to a partition id computed from the hash of the key field value")
        .required(false)
        .defaultValue(FieldDictionary.RECORD_ID)
        .build
}

abstract class AbstractKafkaRecordStream extends AbstractRecordStream with KafkaRecordStream {


    private val logger = LoggerFactory.getLogger(classOf[AbstractKafkaRecordStream])
    protected var kafkaSink: Broadcast[KafkaSink] = null
    protected var zkSink: Broadcast[ZookeeperSink] = null
    protected var controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink] = null
    protected var appName: String = ""
    @transient protected var ssc: StreamingContext = null
    protected var streamContext: StreamContext = null
    protected var engineContext: EngineContext = null

    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(AbstractKafkaRecordStream.ERROR_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.INPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.OUTPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.AVRO_INPUT_SCHEMA)
        descriptors.add(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA)
        descriptors.add(AbstractKafkaRecordStream.INPUT_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.OUTPUT_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.ERROR_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_AUTOCREATE)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_MESSAGE_KEY_FIELD)
        Collections.unmodifiableList(descriptors)
    }


    override def setup(appName: String, ssc: StreamingContext, streamContext: StreamContext, engineContext: EngineContext) = {
        this.appName = appName
        this.ssc = ssc
        this.streamContext = streamContext
        this.engineContext = engineContext

    }

    override def start() = {
        if (ssc == null)
            throw new IllegalStateException("stream not initialized")

        try {

            // Define the Kafka parameters, broker list must be specified
            val inputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_TOPICS).asString.split(",").toSet
            val outputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_TOPICS).asString.split(",").toSet
            val errorTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.ERROR_TOPICS).asString.split(",").toSet
            val topicAutocreate = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_TOPIC_AUTOCREATE).asBoolean().booleanValue()
            val topicDefaultPartitions = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS).asInteger().intValue()
            val topicDefaultReplicationFactor = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR).asInteger().intValue()
            val brokerList = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST).asString
            val zkQuorum = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM).asString
            val zkClient = new ZkClient(zkQuorum, 30000, 30000, ZKStringSerializer)
            val keyField = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_MESSAGE_KEY_FIELD).asString

            val kafkaSinkParams = Map(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
                ProducerConfig.CLIENT_ID_CONFIG -> appName,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getCanonicalName,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
                ProducerConfig.ACKS_CONFIG -> "all",
                ProducerConfig.RETRIES_CONFIG -> "3",
                ProducerConfig.BATCH_SIZE_CONFIG -> "500",
                ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "1000",
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "1000")

            kafkaSink = ssc.sparkContext.broadcast(KafkaSink(kafkaSinkParams, keyField))
            zkSink = ssc.sparkContext.broadcast(ZookeeperSink(zkQuorum))
            controllerServiceLookupSink = ssc.sparkContext.broadcast(
                ControllerServiceLookupSink(engineContext.getControllerServiceConfigurations)
            )

            if (topicAutocreate) {
                createTopicsIfNeeded(zkClient, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkClient, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkClient, errorTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
            }

            /**
              * create streams from latest zk offsets
              *
              * previous values :
              *    refresh.leader.backoff.ms -> 1000
              */
            var kafkaStreamsParams = Map(
              "metadata.broker.list" -> brokerList,
              "bootstrap.servers" -> brokerList,
              "group.id" -> appName,
              "refresh.leader.backoff.ms" -> "5000"
            )
            val autoOffsetResetProp = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET)
            val autoOffsetReset = if (autoOffsetResetProp.isSet) {
                val value = autoOffsetResetProp.asString
                if ( ! AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET.validate(value).isValid ) {
                    logger.error(s"Invalid value '${value}' for property ${AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET.getName()}")
                    throw new IllegalStateException(s"Invalid value '${value}' for property ${AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET.getName()}");
                }
                value
            }
            else {
                null
            }

            if ( autoOffsetReset != null ) {
                kafkaStreamsParams = kafkaStreamsParams + (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> autoOffsetReset)
            }

            val offsets = zkSink.value.loadOffsetRangesFromZookeeper(brokerList, appName, inputTopics)
            @transient val kafkaStream = if (
                streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET).isSet
                    || offsets.isEmpty) {

                logger.info(s"starting Kafka direct stream on topics $inputTopics from offsets $autoOffsetReset")
                KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
                    ssc,
                    kafkaStreamsParams,
                    inputTopics
                )
            }
            else {
                val messageHandler: MessageAndMetadata[Array[Byte], Array[Byte]] => (Array[Byte], Array[Byte]) =
                    (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)

                logger.info(s"starting Kafka direct stream on topics $inputTopics from offsets $offsets")
                KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, (Array[Byte], Array[Byte])](
                    ssc,
                    kafkaStreamsParams,
                    offsets,
                    messageHandler)
            }


            // do the parallel processing
            kafkaStream.foreachRDD(rdd => process(rdd))
        } catch {
            case ex: Throwable => logger.error("something bad happened, please check Kafka or Zookeeper health : {}",
                ex)
        }
    }

    /*
        override def stop() {
            kafkaSink.value.shutdown()
            zkSink.value.shutdown()
        }
      */

    /**
      * to be overriden by subclasses
      *
      * @param rdd
      */
    def process(rdd: RDD[(Array[Byte], Array[Byte])])


    /**
      *
      * @param partition
      * @param serializer
      * @return
      */
    def deserializeRecords(partition: Iterator[(Array[Byte], Array[Byte])], serializer: RecordSerializer): List[Record] = {
        partition.flatMap(rawEvent => {

            try {
                val bais = new ByteArrayInputStream(rawEvent._2)
                val deserialized: Record = serializer.deserialize(bais)
                bais.close()

                Some(deserialized)
            } catch {
                case t: Throwable =>
                    logger.error(s"exception while deserializing events ${t.getMessage}")
                    None
            }

        }).toList
    }


    /**
      * Topic creation
      *
      * @param zkClient
      * @param inputTopics
      * @param topicDefaultPartitions
      * @param topicDefaultReplicationFactor
      */
    def createTopicsIfNeeded(zkClient: ZkClient,
                             inputTopics: Set[String],
                             topicDefaultPartitions: Int,
                             topicDefaultReplicationFactor: Int): Unit = {

        inputTopics.foreach(topic => {
            if (!AdminUtils.topicExists(zkClient, topic)) {
                AdminUtils.createTopic(zkClient, topic, topicDefaultPartitions, topicDefaultReplicationFactor)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with" +
                    s" $topicDefaultPartitions partitions and" +
                    s" $topicDefaultReplicationFactor replicas")
            }
        })
    }
}


