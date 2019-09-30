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
import com.hurence.logisland.record.Record
import com.hurence.logisland.serializer.SerializerProvider
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties.{AVRO_OUTPUT_SCHEMA, WRITE_TOPICS_KEY_SERIALIZER, WRITE_TOPICS_SERIALIZER}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, ForeachWriter, SparkSession}

@CapabilityDescription("Provide a ways to print output in console in a StructuredStream streams")
class ConsoleStructuredStreamProviderService extends AbstractControllerService with StructuredStreamProviderService {

    val NUM_ROWS_TO_SHOW: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("rows")
      .description("Number of rows to print every trigger (default: 20 see spark documentation)")
      .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
      .required(true)
      .build

    val TRUNCATE_OUTPUT: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("truncate")
      .description("Whether to truncate the output if too long (default: true see spark documentation) ")
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .required(false)
      .build

    var  numRows: Option[Long] = _
    var  truncate: Option[Boolean] = _

    @OnEnabled
    @throws[InitializationException]
    override def init(context: ControllerServiceInitializationContext): Unit = {
        super.init(context)
        this.synchronized {
            try {
                if (context.getPropertyValue(NUM_ROWS_TO_SHOW).isSet) {
                    numRows = Some(context.getPropertyValue(NUM_ROWS_TO_SHOW).asLong())
                } else {
                    numRows = None
                }
                if (context.getPropertyValue(TRUNCATE_OUTPUT).isSet) {
                    truncate = Some(context.getPropertyValue(TRUNCATE_OUTPUT).asBoolean())
                } else {
                    truncate = None
                }
            } catch {
                case e: Exception =>
                    throw new InitializationException(e)
            }
        }
    }

    /**
      * Allows subclasses to register which property descriptor objects are
      * supported.
      *
      * @return PropertyDescriptor objects this processor currently supports
      */
    override def getSupportedPropertyDescriptors() = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

        Collections.unmodifiableList(descriptors)
    }

    /**
      * create a streaming DataFrame that represents data received
      *
      * @param spark
      * @param streamContext
      * @return DataFrame currently loaded
      */
    override def read(spark: SparkSession, streamContext: StreamContext) = {
        throw new IllegalArgumentException("ConsoleStructuredStreamProviderService class does not support read operation yet");
    }

    /**
      * create a streaming DataFrame that represents data received
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    override def save(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext): StreamingQuery = {

        // make sure controller service lookup won't be serialized !!
        streamContext.setControllerServiceLookup(null)

        // create serializer
        val serializer = SerializerProvider.getSerializer(
            streamContext.getPropertyValue(WRITE_TOPICS_SERIALIZER).asString,
            streamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)

        // create serializer
        val keySerializer = SerializerProvider.getSerializer(
            streamContext.getPropertyValue(WRITE_TOPICS_KEY_SERIALIZER).asString, null)

        // do the parallel processing
        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]
        val df2 = df.mapPartitions(record => record.map(record => serializeRecords(serializer, keySerializer, record)))


        write(df2, controllerServiceLookupSink, streamContext)
          .queryName(streamContext.getIdentifier)
          // .outputMode("update")
          .foreach(new ForeachWriter[Record] {
            def open(partitionId: Long, version: Long): Boolean = {
                // open connection
                true
            }

            def process(record: Record) = {
                println(record)
                // write string to connection
            }

            def close(errorOrNull: Throwable): Unit = {
                // close the connection
            }
        }).start()

        // .processAllAvailable()

    }
    /**
      * create a streaming DataFrame that represents data received
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext): DataStreamWriter[Record] = {
//        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]

        val dataStreamWriter =  df.writeStream
          .format("console")
        if (numRows.isDefined) {
            dataStreamWriter.option("numRows", numRows.get)
        }
        if (truncate.isDefined) {
            dataStreamWriter.option("truncate", truncate.get)
        }
        dataStreamWriter
    }
}
