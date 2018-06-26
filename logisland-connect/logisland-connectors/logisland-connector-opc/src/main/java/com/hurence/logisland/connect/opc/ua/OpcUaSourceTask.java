/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.connect.opc.ua;

import com.hurence.logisland.connect.opc.CommonUtils;
import com.hurence.logisland.connect.opc.OpcRecordFields;
import com.hurence.logisland.connect.opc.SmartOpcOperations;
import com.hurence.logisland.connect.opc.TagInfo;
import com.hurence.logisland.connect.opc.da.OpcDaSourceConnector;
import com.hurence.opc.OpcTagInfo;
import com.hurence.opc.auth.Credentials;
import com.hurence.opc.auth.UsernamePasswordCredentials;
import com.hurence.opc.ua.OpcUaConnectionProfile;
import com.hurence.opc.ua.OpcUaSession;
import com.hurence.opc.ua.OpcUaSessionProfile;
import com.hurence.opc.ua.OpcUaTemplate;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * OPC-UA source task.
 *
 * @author amarziali
 */
public class OpcUaSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(OpcUaSourceTask.class);

    private SmartOpcOperations<OpcUaConnectionProfile, OpcUaSessionProfile, OpcUaSession> opcOperations;
    private TransferQueue<SourceRecord> transferQueue;
    private String tags[];
    private URI serverUri;
    private long defaultRefreshPeriodMillis;
    private long defaultPublicationPeriodMillis;
    private Map<String, TagInfo> tagInfoMap;
    private ExecutorService executorService;
    private volatile boolean running;


    private OpcUaConnectionProfile propertiesToConnectionProfile(Map<String, String> properties) {
        OpcUaConnectionProfile ret = new OpcUaConnectionProfile();
        ret.setConnectionUri(URI.create(properties.get(OpcUaSourceConnector.PROPERTY_URI)));
        if (properties.containsKey(OpcUaSourceConnector.PROPERTY_AUTH_USER)) {
            ret.setCredentials(new UsernamePasswordCredentials()
                    .withUser(properties.get(OpcUaSourceConnector.PROPERTY_AUTH_USER))
                    .withPassword(properties.get(OpcUaSourceConnector.PROPERTY_AUTH_PASSWORD)));
        } else {
            ret.setCredentials(Credentials.ANONYMOUS_CREDENTIALS);
        }
        if (properties.containsKey(OpcUaSourceConnector.PROPERTY_SOCKET_TIMEOUT)) {
            ret.setSocketTimeout(Duration.ofMillis(Long.parseLong(properties.get(OpcDaSourceConnector.PROPERTY_SOCKET_TIMEOUT))));
        }
        return ret;
    }


    @Override
    public void start(Map<String, String> props) {
        transferQueue = new LinkedTransferQueue<>();
        opcOperations = new SmartOpcOperations<>(new OpcUaTemplate());
        OpcUaConnectionProfile connectionProfile = propertiesToConnectionProfile(props);
        tags = props.get(OpcDaSourceConnector.PROPERTY_TAGS).split(",");
        serverUri = connectionProfile.getConnectionUri();
        defaultRefreshPeriodMillis = Long.parseLong(props.get(OpcUaSourceConnector.PROPERTY_DEFAULT_REFRESH_PERIOD));
        defaultPublicationPeriodMillis = Long.parseLong(props.get(OpcUaSourceConnector.PROPERTY_DATA_PUBLICATION_PERIOD));

        opcOperations.connect(connectionProfile);
        if (!opcOperations.awaitConnected()) {
            throw new ConnectException("Unable to connect");
        }
        Map<String, OpcTagInfo> dictionary =
                opcOperations.fetchMetadata(Arrays.stream(tags).map(t -> CommonUtils.parseTag(t, defaultRefreshPeriodMillis).getKey())
                        .toArray(size -> new String[size]))
                        .stream()
                        .collect(Collectors.toMap(OpcTagInfo::getId, Function.identity()));
        tagInfoMap = Arrays.stream(tags).map(t -> new TagInfo(t, defaultRefreshPeriodMillis, dictionary))
                .collect(Collectors.toMap(t -> t.getTagInfo().getId(), Function.identity()));
        executorService = Executors.newSingleThreadExecutor();
        running = true;
        executorService.submit(() -> {
            final OpcUaSessionProfile sessionProfile = new OpcUaSessionProfile()
                    .withDefaultPollingInterval(Duration.ofMillis(defaultRefreshPeriodMillis))
                    .withRefreshPeriod(Duration.ofMillis(defaultPublicationPeriodMillis));
            tagInfoMap.forEach((n, t) -> sessionProfile.addToPollingMap(t.getTagInfo().getId(), Duration.ofMillis(t.getRefreshPeriodMillis())));
            while (running) {
                try (OpcUaSession session = opcOperations.createSession(sessionProfile)) {
                    session.stream(tagInfoMap.keySet().toArray(new String[tagInfoMap.size()]))
                            .map(opcData -> {
                                        TagInfo meta = tagInfoMap.get(opcData.getTag());
                                        SchemaAndValue dataSchema = CommonUtils.convertToNativeType(opcData.getValue());
                                        Schema valueSchema = CommonUtils.buildSchema(dataSchema.schema());
                                        Struct valueStruct = CommonUtils.mapToConnectObject(opcData,
                                                meta,
                                                valueSchema,
                                                dataSchema,
                                                Collections.singletonMap(OpcRecordFields.OPC_SERVER_HOST, serverUri.toASCIIString()));


                                        Map<String, String> partition = new HashMap<>();
                                        partition.put(OpcRecordFields.TAG_NAME, opcData.getTag());
                                        partition.put(OpcRecordFields.OPC_SERVER_HOST, serverUri.toASCIIString());

                                        return new SourceRecord(
                                                partition,
                                                Collections.singletonMap(OpcRecordFields.TIMESTAMP, opcData.getTimestamp().toEpochMilli()),
                                                "",
                                                SchemaBuilder.STRING_SCHEMA,
                                                serverUri.toASCIIString() + "|" + opcData.getTag(),
                                                valueSchema,
                                                valueStruct);
                                    }
                            )
                            .forEach(transferQueue::add);

                } catch (Exception e) {
                    logger.error("Unexpected exception while streaming tags. Looping again.", e);
                }
            }
            logger.info("OPC-UA reading loop ended.");
        });
        logger.info("Started OPC-UA task for tags {}", (Object) tags);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> ret = new ArrayList<>();
        if (transferQueue.isEmpty()) {
            Thread.sleep(defaultPublicationPeriodMillis);
        }
        transferQueue.drainTo(ret);
        return ret;
    }

    @Override
    public void stop() {
        running = false;
        //session are automatically cleaned up and detached when the connection is closed.

        if (opcOperations != null) {
            opcOperations.disconnect();
            opcOperations.awaitDisconnected();
        }

        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }
        transferQueue = null;
        tagInfoMap = null;
        logger.info("Stopped OPC-UA task for tags {}", (Object) tags);
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

}