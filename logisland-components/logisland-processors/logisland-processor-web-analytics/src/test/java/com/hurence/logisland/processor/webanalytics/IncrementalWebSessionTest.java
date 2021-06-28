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
package com.hurence.logisland.processor.webanalytics;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.webanalytics.modele.TestMappings;
import com.hurence.logisland.processor.webanalytics.modele.WebSession;
import com.hurence.logisland.processor.webanalytics.util.WebEvent;
import com.hurence.logisland.processor.webanalytics.util.WebSessionChecker;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.cache.LRUKeyValueCacheService;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.model.*;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webanalytics.IncrementalWebSession.*;
import static com.hurence.logisland.processor.webanalytics.util.ElasticsearchServiceUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IncrementalWebSessionTest {

    private static final String SESSIONID = "SESSIONID";
    private static final String SESSION_ID = "sessionId";
    private static final String TIMESTAMP = "h2kTimestamp";
    private static final String VISITED_PAGE = "VISITED_PAGE";
    private static final String CURRENT_CART = "currentCart";
    private static final String USER_ID = "Userid";

    private static final String SESSION1 = "session1";
    private static final String SESSION2 = "session2";

    private static final long SESSION_TIMEOUT_SECONDS = 1800L; // but should be 30*60;

    private static final String URL1 = "http://page1";
    private static final String URL2 = "http://page2";
    private static final String URL3 = "http://page3";

    private static final String USER1 = "user1";
    private static final String USER2 = "user2";

    private static final String PARTY_ID1 = "partyId1";

    private static final Long DAY1 = 1493197966584L;  // Wed Apr 26 11:12:46 CEST 2017
    private static final Long DAY2 = 1493297966584L;  // Thu Apr 27 14:59:26 CEST 2017

    private static final String PARTY_ID = "partyId";
    private static final String B2BUNIT = "B2BUnit";

    private static final String INDEX_SESSION_DAY1 = SESSION_INDEX_PREFIX + "2017.04.26";
    private static final String INDEX_SESSION_DAY2 = SESSION_INDEX_PREFIX + "2017.04.27";
    private static final String INDEX_EVENT_DAY1 = EVENT_INDEX_PREFIX + "2017.04.26";
    private static final String INDEX_EVENT_DAY2 = EVENT_INDEX_PREFIX + "2017.04.27";
    private static final String SESSION_TYPE = "sessions";
    private static final String EVENT_TYPE = "event";
    public static final int EVENT_NUMBER_OF_FIELD_ORIGINAL_SESSION = 7;
    public static final int EVENT_NUMBER_OF_FIELD_NEW_SESSION = 9;

    @Test
    public void testValidity()
    {
        IncrementalWebSession proc = new IncrementalWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.assertNotValid();

        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd");
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.assertValid();

        runner.removeProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.removeProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.removeProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM");
        runner.removeProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd");
        runner.removeProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.removeProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.removeProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.removeProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF);
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.assertValid();

        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "aba");
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "Canada/Yukon");
        runner.assertValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "CaNAda/YuKON");
        runner.assertNotValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "UTC");
        runner.assertValid();
        runner.setProperty(IncrementalWebSession.ZONEID_CONF, "Japan");
        runner.assertValid();
    }


    @Test
    public void testChoosingTheZoneId() {
        //        1575158400000    1/12/2019 à 0:00:00   "1/12/2019 à 1:00:00 +0100"  in LOCAL english
        //        1577836799000    31/12/2019 à 23:59:59 "1/1/2020 à 0:59:59 +0100"  in LOCAL english
        //        1577836800000    1/1/2020 à 0:00:00    "1/1/2020 à 1:00:00 +0100" in LOCAL english
        //        1577840399000    1/1/2020 à 0:59:59    "1/1/2020 à 1:59:59 +0100" in LOCAL english
        //        1580515199000    31/1/2020 à 23:59:59  "1/2/2020 à 0:59:59 +0100" in LOCAL english
        //        1580515200000    1/2/2020 à 0:00:00    "1/2/2020 à 1:00:00 +0100" in LOCAL english
        //        1583020799000    29/2/2020 à 23:59:59  "1/3/2020 à 0:59:59 +0100" in LOCAL english
        final long time1 = 1575158400000L;
        final long time2 = 1577836799000L;
        final long time3 = 1577836800000L;
        final long time4 = 1577840399000L;
        final long time5 = 1580515199000L;
        final long time6 = 1580515200000L;
        final long time7 = 1583020799000L;
        IncrementalWebSession proc = new IncrementalWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd HH:mm:ss Z");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, "yyyy.MM.dd HH:mm:ss Z");
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, SESSION_TYPE);
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, EVENT_TYPE);
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.assertValid();
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.01 01:00:00 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.01 01:00:00 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 00:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 00:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 01:00:00 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 01:00:00 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 01:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 01:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 00:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 00:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 01:00:00 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 01:00:00 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.03.01 00:59:59 +0100", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.03.01 00:59:59 +0100", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));


        runner.setProperty(IncrementalWebSession.PROP_ES_INDEX_SUFFIX_TIMEZONE, "Japan");
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.01 09:00:00 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.01 09:00:00 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 08:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 08:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 09:00:00 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 09:00:00 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 09:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 09:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 08:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 08:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 09:00:00 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 09:00:00 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.03.01 08:59:59 +0900", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.03.01 08:59:59 +0900", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));

        runner.setProperty(IncrementalWebSession.PROP_ES_INDEX_SUFFIX_TIMEZONE,  "Canada/Yukon");
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.11.30 16:00:00 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.11.30 16:00:00 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 15:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 15:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 16:00:00 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 16:00:00 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 16:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 16:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.31 15:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.31 15:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.31 16:00:00 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.31 16:00:00 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.29 15:59:59 -0800", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.29 15:59:59 -0800", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));

        runner.setProperty(IncrementalWebSession.PROP_ES_INDEX_SUFFIX_TIMEZONE,  "UTC");
        runner.run();
        // 1/12/2019 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.01 00:00:00 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time1)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.01 00:00:00 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time1)));
        //"1/1/2020 à 0:59:59 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2019.12.31 23:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time2)));
        assertEquals(SESSION_INDEX_PREFIX +"2019.12.31 23:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time2)));
        //1/1/2020 à 1:00:00 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 00:00:00 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time3)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 00:00:00 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time3)));
        //1/1/2020 à 1:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.01 00:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time4)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.01 00:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time4)));
        //1/2/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.01.31 23:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time5)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.01.31 23:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time5)));
        //"1/2/2020 à 1:00:00 +0100"
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.01 00:00:00 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time6)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.01 00:00:00 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time6)));
        //1/3/2020 à 0:59:59 +0100
        assertEquals(EVENT_INDEX_PREFIX+   "2020.02.29 23:59:59 +0000", proc.toEventIndexName(GetZonedDateTimeFromEpochMili(time7)));
        assertEquals(SESSION_INDEX_PREFIX +"2020.02.29 23:59:59 +0000", proc.toSessionIndexName(GetZonedDateTimeFromEpochMili(time7)));
    }

    @Test
    public void testAdword()
            throws Exception
    {
        String URL = URL1+"?gclid=XXX";
        WebEvent event1 = new WebEvent("1", SESSION1, USER1, DAY1, URL);
        WebEvent event2 = new WebEvent("2", SESSION1, USER1, DAY2, URL2);

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(event1, event2));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // Two webSessions expected .
        testRunner.assertOutputRecordsCount(4);
        List<MockRecord> outputRecords = testRunner.getOutputRecords();
        MockRecord session1 = getFirstRecordWithId(SESSION1, outputRecords);
        session1.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_SESSION_DAY1);
        session1.assertFieldEquals(defaultOutputFieldNameForEsType, SESSION_TYPE);
        new WebSessionChecker(session1).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
        MockRecord session2 = getFirstRecordWithId(SESSION1+"#2", outputRecords);
        session2.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_SESSION_DAY2);
        session2.assertFieldEquals(defaultOutputFieldNameForEsType, SESSION_TYPE);
        new WebSessionChecker(session2).sessionId(SESSION1+"#2")
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1+"#2")
                .firstEventDateTime(DAY2)
                .h2kTimestamp(DAY2)
                .firstVisitedPage(URL2)
                .eventsCounter(1)
                .lastEventDateTime(DAY2)
                .lastVisitedPage(URL2)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);


        MockRecord event1Output = getFirstRecordWithId("1", outputRecords);
        MockRecord event2Output = getFirstRecordWithId("2", outputRecords);

        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getUserIdField(), USER1);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getVisitedPageField(), URL);
        event1Output.assertNullField(WebEvent.CURRENT_CART);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_EVENT_DAY1);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsType, EVENT_TYPE);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getTimestampField(), DAY1);
        event1Output.assertRecordSizeEquals(EVENT_NUMBER_OF_FIELD_ORIGINAL_SESSION);

        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getOriginalSessionIdField(), SESSION1);
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1+"#2");
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getNewSessionReasonField(), DAY_OVERLAP.reason());
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getUserIdField(), USER1);
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getVisitedPageField(), URL2);
        event2Output.assertNullField(WebEvent.CURRENT_CART);
        event2Output.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_EVENT_DAY2);
        event2Output.assertFieldEquals(defaultOutputFieldNameForEsType, EVENT_TYPE);
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getTimestampField(), DAY2);
        event2Output.assertRecordSizeEquals(EVENT_NUMBER_OF_FIELD_NEW_SESSION);
    }

    @Test
    public void testEventHandleCorrectlyNullArrays()
            throws Exception
    {

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        WebEvent inputEvent = new WebEvent("1", SESSION1, USER1, DAY1, URL1);
        testRunner.enqueue(Arrays.asList(inputEvent));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(2);

        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        session.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_SESSION_DAY1);
        session.assertFieldEquals(defaultOutputFieldNameForEsType, SESSION_TYPE);
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .currentCart(null)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

        MockRecord event1Output = getFirstRecordWithId("1", testRunner.getOutputRecords());
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getUserIdField(), USER1);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getVisitedPageField(), URL1);
        event1Output.assertNullField(WebEvent.CURRENT_CART);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_EVENT_DAY1);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsType, EVENT_TYPE);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getTimestampField(), DAY1);
        event1Output.assertRecordSizeEquals(EVENT_NUMBER_OF_FIELD_ORIGINAL_SESSION);
    }


    @Test
    public void testCreateOneSessionOneEvent()
            throws Exception
    {
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("1", SESSION1, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        session.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_SESSION_DAY1);
        session.assertFieldEquals(defaultOutputFieldNameForEsType, SESSION_TYPE);
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

        MockRecord event1Output = getFirstRecordWithId("1", testRunner.getOutputRecords());
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getUserIdField(), USER1);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getVisitedPageField(), URL1);
        event1Output.assertNullField(WebEvent.CURRENT_CART);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_EVENT_DAY1);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsType, EVENT_TYPE);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getTimestampField(), DAY1);
        event1Output.assertRecordSizeEquals(EVENT_NUMBER_OF_FIELD_ORIGINAL_SESSION);
    }

    @Test
    public void testCreateOneSessionMultipleEvents()
            throws Exception
    {
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("1", SESSION1, null, DAY1, URL1),
                new WebEvent("2", SESSION1, USER1, DAY1+1000L, URL2),
                new WebEvent("3", SESSION1, null, DAY1+2000L, URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // One webSession expected.
        testRunner.assertOutputRecordsCount(4);
        final MockRecord doc = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(doc).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL1)
                .eventsCounter(3)
                .lastEventDateTime(DAY1+2000L)
                .lastVisitedPage(URL3)
                .sessionDuration(2L)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

        MockRecord event1Output = getFirstRecordWithId("1", testRunner.getOutputRecords());
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1);
        event1Output.assertNullField(TestMappings.eventsInternalFields.getUserIdField());
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getVisitedPageField(), URL1);
        event1Output.assertNullField(WebEvent.CURRENT_CART);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_EVENT_DAY1);
        event1Output.assertFieldEquals(defaultOutputFieldNameForEsType, EVENT_TYPE);
        event1Output.assertFieldEquals(TestMappings.eventsInternalFields.getTimestampField(), DAY1);
        event1Output.assertRecordSizeEquals(EVENT_NUMBER_OF_FIELD_ORIGINAL_SESSION);

        MockRecord event2Output = getFirstRecordWithId("2", testRunner.getOutputRecords());
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1);
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getUserIdField(), USER1);
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getVisitedPageField(), URL2);
        event2Output.assertNullField(WebEvent.CURRENT_CART);
        event2Output.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_EVENT_DAY1);
        event2Output.assertFieldEquals(defaultOutputFieldNameForEsType, EVENT_TYPE);
        event2Output.assertFieldEquals(TestMappings.eventsInternalFields.getTimestampField(), DAY1+1000L);
        event2Output.assertRecordSizeEquals(EVENT_NUMBER_OF_FIELD_ORIGINAL_SESSION);

        MockRecord event3Output = getFirstRecordWithId("3", testRunner.getOutputRecords());
        event3Output.assertFieldEquals(TestMappings.eventsInternalFields.getSessionIdField(), SESSION1);
        event3Output.assertNullField(TestMappings.eventsInternalFields.getUserIdField());
        event3Output.assertFieldEquals(TestMappings.eventsInternalFields.getVisitedPageField(), URL3);
        event3Output.assertNullField(WebEvent.CURRENT_CART);
        event3Output.assertFieldEquals(defaultOutputFieldNameForEsIndex, INDEX_EVENT_DAY1);
        event3Output.assertFieldEquals(defaultOutputFieldNameForEsType, EVENT_TYPE);
        event3Output.assertFieldEquals(TestMappings.eventsInternalFields.getTimestampField(), DAY1+2000L);
        event3Output.assertRecordSizeEquals(EVENT_NUMBER_OF_FIELD_ORIGINAL_SESSION);
    }

    @Test
    public void testCreateOneSessionMultipleEventsData2()
            throws Exception
    {
        Object[] data = new Object[]{
                new Object[]{
                        "h2kTimestamp" , 1538753339113L,
                        "location" , "https://www.zitec-shop.com/en/",
                        "sessionId" , SESSIONID // "0:jmw62hh3:ef5kWpbpKDNBG5IyGBARUQLemW3JP0PP"
                }, new Object[]{
                "h2kTimestamp" , 1538753689109L,
                "location" , "https://www.zitec-shop.com/en/",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538753753964L,
                "location" , "https://www.zitec-shop.com/en/",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538753768489L,
                "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756201154L, // timeout
                "location" , "https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756215043L,
                "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756216242L,
                "location" , "https://www.zitec-shop.com/en/search/?text=rotex%2Cgg%2C48",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756232483L,
                "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756400671L,
                "location" , "https://www.zitec-shop.com/en/rotex-48-gg/p-G1184000392",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538756417237L,
                "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767062429L, // timeout
                "location" , "https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767070188L,
                "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767073907L,
                "location" , "https://www.zitec-shop.com/en/search/?text=nah25",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538767077273L,
                "location" , "https://www.zitec-shop.com/en/search/?text=nah",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780539746L, // timeout
                "location" , "https://www.zitec-shop.com/en/search/?text=nah",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780546243L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780578259L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780595932L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780666747L,
                "location" , "https://www.zitec-shop.com/en/search/?text=hj+234",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780680777L,
                "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780684232L,
                "location" , "https://www.zitec-shop.com/en/login",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780691752L,
                "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780748044L,
                "location" , "https://www.zitec-shop.com/en/sealed-spherical-roller-bearings-ws222-e1/p-G1112006937",
                "sessionId" , SESSIONID
        }, new Object[]{
                "h2kTimestamp" , 1538780763016L,
                "location" , "https://www.zitec-shop.com/en/roller-bearing-spherical-radial-multi-row/p-G1321019550",
                "sessionId" , SESSIONID}};

        TestRunner testRunner = newTestRunner();
        testRunner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, "1800");
        testRunner.assertValid();
        List<Record> events = new ArrayList<>(3);
        int eventCounter = 0;
        for(final Object line: data)
        {
            final Iterator iterator = Arrays.asList((Object[])line).iterator();
            final Map fields = new HashMap();
            while (iterator.hasNext())
            {
                Object name = iterator.next();
                Object value = iterator.next();
                fields.put(name, value);
            }
            events.add(new WebEvent(String.valueOf(eventCounter++),
                    (String)fields.get("sessionId"),
                    (String)fields.get("userId"),
                    (Long)fields.get("h2kTimestamp"),
                    (String)fields.get("location")));
        }
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        testRunner.assertOutputRecordsCount(data.length + 4);

        final MockRecord session1 = getFirstRecordWithId(SESSIONID, testRunner.getOutputRecords());
        new WebSessionChecker(session1).sessionId(SESSIONID)
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID)
                .firstEventDateTime(1538753339113L)
                .h2kTimestamp(1538753339113L)
                .firstVisitedPage("https://www.zitec-shop.com/en/")
                .eventsCounter(4)
                .lastEventDateTime(1538753768489L)
                .lastVisitedPage("https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137")
                .sessionDuration(429L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
        final MockRecord session2 = getFirstRecordWithId(SESSIONID+ "#2", testRunner.getOutputRecords());
        new WebSessionChecker(session2).sessionId(SESSIONID + "#2")
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID + "#2")
                .firstEventDateTime(1538756201154L)
                .h2kTimestamp(1538756201154L)
                .firstVisitedPage("https://www.zitec-shop.com/en/schragkugellager-718-tn/p-G1156005137")
                .eventsCounter(6)
                .lastEventDateTime(1538756417237L)
                .lastVisitedPage("https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584")
                .sessionDuration(216L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
        final MockRecord session3 = getFirstRecordWithId(SESSIONID+ "#3", testRunner.getOutputRecords());
        new WebSessionChecker(session3).sessionId(SESSIONID + "#3")
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID + "#3")
                .firstEventDateTime(1538767062429L)
                .h2kTimestamp(1538767062429L)
                .firstVisitedPage("https://www.zitec-shop.com/en/kugelfuhrungswagen-vierreihig/p-G1156007584")
                .eventsCounter(4)
                .lastEventDateTime(1538767077273L)
                .lastVisitedPage("https://www.zitec-shop.com/en/search/?text=nah")
                .sessionDuration(14L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
        final MockRecord session4 = getFirstRecordWithId(SESSIONID+ "#4", testRunner.getOutputRecords());
        new WebSessionChecker(session4).sessionId(SESSIONID + "#4")
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(SESSIONID + "#4")
                .firstEventDateTime(1538780539746L)
                .h2kTimestamp(1538780539746L)
                .firstVisitedPage("https://www.zitec-shop.com/en/search/?text=nah")
                .eventsCounter(10)
                .lastEventDateTime(1538780763016L)
                .lastVisitedPage("https://www.zitec-shop.com/en/roller-bearing-spherical-radial-multi-row/p-G1321019550")
                .sessionDuration(223L)
                .is_sessionActive(false)
                .sessionInactivityDuration(1800L);
    }

    @Test
    public void testCreateOneSessionMultipleEventsData()
            throws Exception
    {
        Object[] data = new Object[]{
                new Object[]{"sessionId", SESSION1,
                        "partyId", PARTY_ID1,
                        "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                        "h2kTimestamp", 1529673800671L,
                        "userId", null},
                new Object[]{"sessionId", SESSION1,
                        "partyId", PARTY_ID1,
                        "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10?utm_source=TEST&utm_medium=email&utm_campaign=HT",
                        "h2kTimestamp", 1529673855863L,
                        "userId", null},
                new Object[]{"sessionId", SESSION1,
                        "partyId", PARTY_ID1,
                        "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                        "h2kTimestamp", 1529673912936L,
                        "userId", null}};
        int eventCount = 0;
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        List<Record> events = new ArrayList<>(3);
        for(final Object line: data)
        {
            final Iterator iterator = Arrays.asList((Object[])line).iterator();
            final Map fields = new HashMap();
            while (iterator.hasNext())
            {
                Object name = iterator.next();
                Object value = iterator.next();
                fields.put(name, value);
            }
            events.add(new WebEvent(String.valueOf(eventCount++),
                    (String)fields.get("sessionId"),
                    (String)fields.get("userId"),
                    (Long)fields.get("h2kTimestamp"),
                    (String)fields.get("location")));
        }
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // One webSession expected.
        testRunner.assertOutputRecordsCount(data.length + 1);

        final Record firstEvent = getFirstRecordWithId("0", testRunner.getOutputRecords());
        final Record lastEvent = getFirstRecordWithId(String.valueOf(data.length - 1), testRunner.getOutputRecords());;

        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session).sessionId(firstEvent.getField(SESSION_ID).getRawValue())
                .Userid(null)
                .record_type("consolidate-session")
                .record_id(firstEvent.getField(SESSION_ID).getRawValue())
                .firstEventDateTime(firstEvent.getField(TIMESTAMP).asLong())
                .h2kTimestamp((Long)firstEvent.getField(TIMESTAMP).getRawValue())
                .firstVisitedPage(firstEvent.getField(VISITED_PAGE).getRawValue())
                .eventsCounter(3)
                .lastEventDateTime(lastEvent.getField(TIMESTAMP).asLong())
                .lastVisitedPage(lastEvent.getField(VISITED_PAGE).getRawValue())
                .sessionDuration((lastEvent.getField(TIMESTAMP).asLong() // lastEvent.getField(TIMESTAMP)
                        -firstEvent.getField(TIMESTAMP).asLong())/1000)
                .is_sessionActive((Instant.now().toEpochMilli()
                        -lastEvent.getField(TIMESTAMP).asLong())/1000< SESSION_TIMEOUT_SECONDS)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
    }

    @Test
    public void testCreateTwoSessionTwoEvents()
            throws Exception
    {
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("1", SESSION1, USER1, DAY1, URL1),
                new WebEvent("2", SESSION2, USER2, DAY2, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // Two webSession expected.
        testRunner.assertOutputRecordsCount(4);

        final MockRecord session1 = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session1).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
        final MockRecord session2 = getFirstRecordWithId(SESSION2, testRunner.getOutputRecords());
        new WebSessionChecker(session2).sessionId(SESSION2)
                .Userid(USER2)
                .record_type("consolidate-session")
                .record_id(SESSION2)
                .firstEventDateTime(DAY2)
                .h2kTimestamp(DAY2)
                .firstVisitedPage(URL2)
                .eventsCounter(1)
                .lastEventDateTime(DAY2)
                .lastVisitedPage(URL2)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);
    }

    @Test
    public void testCreateOneActiveSessionOneEvent()
            throws Exception
    {
        final long now = Instant.now().toEpochMilli();
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("0", SESSION1, USER1, now, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(now)
                .h2kTimestamp(now)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(now)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(true)
                .sessionInactivityDuration(null);
    }

    @Test
    public void testCreateIgnoreOneEventWithoutSessionId()
            throws Exception
    {
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("1", SESSION1, USER1, DAY1, URL1),
                new WebEvent("2", null, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);
        // One webSession expected. Ignoring event without sessionId
        testRunner.assertOutputRecordsCount(2);
        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

    }

    @Test
    public void testCreateIgnoreOneEventWithoutTimestamp()
            throws Exception
    {
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("1", SESSION1, USER1, DAY1, URL1),
                new WebEvent("2", SESSION1, USER1, null, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected .
        testRunner.assertOutputRecordsCount(2);
        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS);

    }

    @Test
    public void testBuggyResume()
            throws Exception
    {
        int eventCount = 0;
        final long DAY1 = 1531492035025L;
        final long TIME2 = 1531494495026L;
        final String URL1 = "https://orexad.preprod.group-iph.com/fr/cart";
        final String URL2 = "https://orexad.preprod.group-iph.com/fr/checkout/single/summary";
        final long SESSION_TIMEOUT = 1800;
        final Collection<Record> events = Arrays.asList(
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, DAY1,
                        URL1),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531492435034L,
                        "https://orexad.preprod.group-iph.com/fr/cart"),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531493380029L,
                        "https://orexad.preprod.group-iph.com/fr/search/?text=Vis"),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531493805028L,
                        "https://orexad.preprod.group-iph.com/fr/search/?text=Vis"),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531493810026L,
                        "https://orexad.preprod.group-iph.com/fr/vis-traction-complete-p-kit-k300/p-G1296007152?l=G1296007152"),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531494175027L,
                        "https://orexad.preprod.group-iph.com/fr/cart"),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531494180026L,
                        "https://orexad.preprod.group-iph.com/fr/cart"),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, 1531494480026L,
                        "https://orexad.preprod.group-iph.com/fr/cart"),
                new WebEvent(String.valueOf(eventCount++), SESSION1, USER1, TIME2,
                        URL2));

        TestRunner testRunner = newTestRunner();
        testRunner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, String.valueOf(SESSION_TIMEOUT));
        testRunner.assertValid();
        testRunner.enqueue(events);

        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected .
        testRunner.assertOutputRecordsCount(events.size() + 1);
        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL1)
                .eventsCounter(events.size())
                .lastEventDateTime(TIME2)
                .lastVisitedPage(URL2)
                .sessionDuration((TIME2-DAY1)/1000)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateGrabOneFieldPresentEveryWhere()
            throws Exception
    {
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent("1", SESSION1, USER1, DAY1, URL1).add(PARTY_ID, PARTY_ID1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord session = getFirstRecordWithId(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(session).sessionId(SESSION1)
                .Userid(USER1)
                .record_type("consolidate-session")
                .record_id(SESSION1)
                .firstEventDateTime(DAY1)
                .h2kTimestamp(DAY1)
                .firstVisitedPage(URL1)
                .eventsCounter(1)
                .lastEventDateTime(DAY1)
                .lastVisitedPage(URL1)
                .sessionDuration(null)
                .is_sessionActive(false)
                .sessionInactivityDuration(SESSION_TIMEOUT_SECONDS)
                .check(PARTY_ID, PARTY_ID1)
                .check(B2BUNIT, null);
    }

    private MockRecord getFirstRecordWithId(final String id, final List<MockRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().get();
    }

    private ZonedDateTime GetZonedDateTimeFromEpochMili(long time1) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(time1), ZoneId.systemDefault());
    }

    /**
     * Creates a new TestRunner set with the appropriate properties.
     *
     * @return a new TestRunner set with the appropriate properties.
     *
     * @throws InitializationException in case the runner could not be instantiated.
     */
    private TestRunner newTestRunner()
            throws InitializationException
    {
        IncrementalWebSession proc = new IncrementalWebSession();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final String FIELDS_TO_RETURN = Stream.of("partyId",  "B2BUnit").collect(Collectors.joining(","));
//        fields.to.return: partyId,Company,remoteHost,tagOrigin,sourceOrigin,spamOrigin,referer,userAgentString,utm_source,utm_campaign,utm_medium,utm_content,utm_term,alert_match_name,alert_match_query,referer_hostname,DeviceClass,AgentName,ImportanceCode,B2BUnit,libelle_zone,Userid,customer_category,source_of_traffic_source,source_of_traffic_medium,source_of_traffic_keyword,source_of_traffic_campaign,source_of_traffic_organic_search,source_of_traffic_content,source_of_traffic_referral_path,websessionIndex
        configureElasticsearchClientService(runner);
        configureCacheService(runner);
        runner.setProperty(IncrementalWebSession.CONFIG_CACHE_SERVICE, "lruCache");
        runner.setProperty(IncrementalWebSession.ELASTICSEARCH_CLIENT_SERVICE_CONF, "elasticsearchClient");
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_PREFIX_CONF, SESSION_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF, SESSION_SUFFIX_FORMATTER_STRING);
        runner.setProperty(IncrementalWebSession.ES_SESSION_TYPE_NAME_CONF, "sessions");
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_PREFIX_CONF, EVENT_INDEX_PREFIX);
        runner.setProperty(IncrementalWebSession.ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF, EVENT_SUFFIX_FORMATTER_STRING);
        runner.setProperty(IncrementalWebSession.ES_EVENT_TYPE_NAME_CONF, "event");
        runner.setProperty(IncrementalWebSession.SESSION_ID_FIELD_CONF, "sessionId");
        runner.setProperty(IncrementalWebSession.TIMESTAMP_FIELD_CONF, "h2kTimestamp");
        runner.setProperty(IncrementalWebSession.VISITED_PAGE_FIELD, "VISITED_PAGE");
        runner.setProperty(IncrementalWebSession.USER_ID_FIELD, "Userid");
        runner.setProperty(IncrementalWebSession.SESSION_INACTIVITY_TIMEOUT_CONF, String.valueOf(SESSION_TIMEOUT_SECONDS));
        runner.setProperty(IncrementalWebSession.FIELDS_TO_RETURN, FIELDS_TO_RETURN);
        runner.setProperty(IncrementalWebSession.IS_SINGLE_PAGE_VISIT_FIELD, "is_single_page_visit");
        return runner;
    }

    private void configureCacheService(TestRunner runner) throws InitializationException {
        final LRUKeyValueCacheService<String, WebSession> cacheService = new LRUKeyValueCacheService<>();
        runner.addControllerService("lruCache", cacheService);
        runner.setProperty(cacheService,
                LRUKeyValueCacheService.CACHE_SIZE, "1000");
        runner.assertValid(cacheService);
        runner.enableControllerService(cacheService);
    }

    private void configureElasticsearchClientService(final TestRunner runner) throws InitializationException {
        final ElasticsearchClientService elasticsearchClientService = new MockEsService();
        runner.addControllerService("elasticsearchClient", elasticsearchClientService);
        runner.assertValid(elasticsearchClientService);
        runner.enableControllerService(elasticsearchClientService);
    }

    public static class MockEsService implements ElasticsearchClientService {

        @Override
        public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId) {

        }

        @Override
        public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {

        }

        @Override
        public void bulkDelete(String docIndex, String docType, String id) {

        }

        @Override
        public void deleteByQuery(QueryRecord queryRecord) throws DatastoreClientServiceException {

        }

        @Override
        public QueryResponseRecord queryGet(QueryRecord queryRecord) throws DatastoreClientServiceException {
            return new QueryResponseRecord(0, Collections.emptyList());
        }

        @Override
        public MultiQueryResponseRecord multiQueryGet(MultiQueryRecord queryRecords) throws DatastoreClientServiceException {
            return new MultiQueryResponseRecord(Collections.emptyList());
        }

        @Override
        public void waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(String[] indices, long timeoutMilli) throws DatastoreClientServiceException {

        }

        @Override
        public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {

        }

        @Override
        public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue) {
            return 0;
        }

        @Override
        public String convertRecordToString(Record record) {
            return "";
        }

        @Override
        public void waitUntilCollectionReady(String name, long timeoutMilli) throws DatastoreClientServiceException {

        }

        @Override
        public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {

        }

        @Override
        public void dropCollection(String name) throws DatastoreClientServiceException {

        }

        @Override
        public long countCollection(String name) throws DatastoreClientServiceException {
            return 0;
        }

        @Override
        public boolean existsCollection(String name) throws DatastoreClientServiceException {
            return false;
        }

        @Override
        public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {

        }

        @Override
        public void createAlias(String collection, String alias) throws DatastoreClientServiceException {

        }

        @Override
        public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
            return false;
        }

        @Override
        public void bulkFlush() throws DatastoreClientServiceException {

        }

        @Override
        public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {

        }

        @Override
        public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {

        }

        @Override
        public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {

        }

        @Override
        public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
            return Collections.emptyList();
        }

        @Override
        public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
            return new StandardRecord();
        }

        @Override
        public Collection<Record> query(String query) {
            return Collections.emptyList();
        }

        @Override
        public long queryCount(String query) {
            return 0;
        }

        @Override
        public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

        }

        @Override
        public Collection<ValidationResult> validate(Configuration context) {
            return Collections.emptyList();
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String name) {
            return null;
        }

        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return Collections.emptyList();
        }

        @Override
        public String getIdentifier() {
            return "elasticsearchClient";
        }
    }
}
