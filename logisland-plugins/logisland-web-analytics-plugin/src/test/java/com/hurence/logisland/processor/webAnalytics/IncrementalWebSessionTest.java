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
package com.hurence.logisland.processor.webAnalytics;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.elasticsearch.ElasticsearchRecordConverter;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test incremental web-session processor.
 */
public class IncrementalWebSessionTest
{
    Object[] data = new Object[]{
        new Object[]{"sessionId", "0:jiq0aaj2:6osgJiqVwdTq8CGHpIRXdeufGD71mxCW",
                     "partyId", "0:jiq0aaj2:plz07Fik~eNyLzMsxTk6tg00YqKkgBvd",
                     "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                     "h2kTimestamp", 1529673800671L,
                     "userId", null},
        new Object[]{"sessionId", "0:jiq0aaj2:6osgJiqVwdTq8CGHpIRXdeufGD71mxCW",
                     "partyId", "0:jiq0aaj2:plz07Fik~eNyLzMsxTk6tg00YqKkgBvd",
                     "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10?utm_source=TEST&utm_medium=email&utm_campaign=HT",
                     "h2kTimestamp", 1529673855863L,
                     "userId", null},
        new Object[]{"sessionId", "0:jiq0aaj2:6osgJiqVwdTq8CGHpIRXdeufGD71mxCW",
                     "partyId", "0:jiq0aaj2:plz07Fik~eNyLzMsxTk6tg00YqKkgBvd",
                     "location", "https://orexad.preprod.group-iph.com/fr/entretien-de-fluides/c-20-50-10",
                     "h2kTimestamp", 1529673912936L,
                     "userId", null}
        };
// {
//      "_index" : "openanalytics_websessions",
//      "_type" : "sessions",
//      "_id" : "0:j4i60fkk:4nOouGJy0uYwLXVraOT34SdFgW6_J7LS",
//      "_score" : 1.0,
//      "_source" : {
//        "@timestamp" : "2017-06-29T10:56:10+02:00",
//        "Userid" : "undefined",
//        "eventsCounter" : 9,
//        "firstEventDateTime" : "Thu Jun 29 10:24:59 CEST 2017",
//        "firstVisitedPage" : "https://www.orexad.com/?gclid=CK-DksbQ4tQCFU8o0wod0qsIHQ",
//        "h2kTimestamp" : 1498724699959,
//        "is_sessionActive" : true,
//        "lastEventDateTime" : "Thu Jun 29 10:27:57 CEST 2017",
//        "lastVisitedPage" : "https://www.orexad.com/fr/lampe-a-souder-lamp-express-classique/p-G3786000051",
//        "partyId" : "0:j4i60fkk:_zDXAEhgjDXWd0XeBCpJt~B2SnHQOYhP",
//        "record_id" : "0:j4i60fkk:4nOouGJy0uYwLXVraOT34SdFgW6_J7LS",
//        "record_time" : 1498726570383,
//        "record_type" : "consolidate-session",
//        "sessionDuration" : 177,
//        "sessionId" : "0:j4i60fkk:4nOouGJy0uYwLXVraOT34SdFgW6_J7LS",
//        "sessionInactivityDuration" : 1692
//      }
//    }
    private static final String INDEX = "openanalytics_websessions";
    private static final String INDEX_BACKUP = "openanalytics_websessions-backup";
    private static final String TYPE = "sessions";

    private static final String SESSION_ID = "sessionId";
    private static final String TIMESTAMP = "h2kTimestamp";
    private static final String VISITED_PAGE = "VISITED_PAGE";
    private static final String USER_ID = "Userid";

    private static final String SESSION1 = "session1";
    private static final String SESSION2 = "session2";

    private static final long SESSION_TIMEOUT = 1800;

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

    private static final String FIELDS_TO_RETURN = Stream.of(PARTY_ID, B2BUNIT).collect(Collectors.joining(","));

    private final ESC elasticsearchClient = new ESC();

    private MockRecord getRecord(final String session, final List<MockRecord> records)
    {
        return records.stream().filter(record -> record.getId().equals(session)).findFirst().get();
    }

    @Test
    public void testCreateOneSessionOneEvent()
        throws Exception
    {
        this.elasticsearchClient.documents.clear();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);

        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);


    }

    @Test
    public void testCreateOneSessionMultipleEvents()
        throws Exception
    {
        this.elasticsearchClient.documents.clear();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(SESSION1, USER1, DAY1+1000L, URL2),
                                         new WebEvent(SESSION1, USER1, DAY1+2000L, URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateOneSessionMultipleEventsData()
        throws Exception
    {
        this.elasticsearchClient.documents.clear();

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
            events.add(new WebEvent((String)fields.get("sessionId"),
                                    (String)fields.get("userId"),
                                    (Long)fields.get("h2kTimestamp"),
                                    (String)fields.get("location")));
        }
        testRunner.enqueue(events);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);

        final Record firstEvent = events.get(0);
        final Record lastEvent = events.get(2);
        final String user = firstEvent.getField(USER_ID)==null?null:firstEvent.getField(USER_ID).asString();

        final MockRecord doc = getRecord((String)firstEvent.getField(SESSION_ID).getRawValue(),
                                         testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(firstEvent.getField(SESSION_ID).getRawValue())
                                  .Userid(user)
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
                                                            -lastEvent.getField(TIMESTAMP).asLong())/1000<SESSION_TIMEOUT)
                                  .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateTwoSessionTwoEvents()
        throws Exception
    {
        this.elasticsearchClient.documents.clear();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(SESSION2, USER2, DAY2, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // Two webSession expected = 4 documents:2 for backup + 2 for current.
        testRunner.assertOutputRecordsCount(4);

        final MockRecord doc1 = getRecord(SESSION1, testRunner.getOutputRecords());
        final MockRecord doc2 = getRecord(SESSION2, testRunner.getOutputRecords());

        new WebSessionChecker(doc1).sessionId(SESSION1)
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
                                   .sessionInactivityDuration(SESSION_TIMEOUT);

        new WebSessionChecker(doc2).sessionId(SESSION2)
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
                                   .sessionInactivityDuration(SESSION_TIMEOUT);
    }

    @Test
    public void testCreateOneActiveSessionOneEvent()
        throws Exception
    {
        this.elasticsearchClient.documents.clear();

        final long now = Instant.now().toEpochMilli();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, now, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
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
        this.elasticsearchClient.documents.clear();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(null, USER1, DAY1, URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);

    }

    @Test
    public void testCreateIgnoreOneEventWithoutTimestamp()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, DAY1, URL1),
                                         new WebEvent(SESSION1, USER1, null, URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
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
                                  .sessionInactivityDuration(SESSION_TIMEOUT);

    }

    @Test
    public void testCreateGrabOneFieldPresentEveryWhere()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, DAY1, URL1).add(PARTY_ID, PARTY_ID1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
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
                                  .sessionInactivityDuration(SESSION_TIMEOUT)
                                  .check(PARTY_ID, PARTY_ID1)
                                  .check(B2BUNIT, null);

    }

    @Test
    public void testCreateGrabTwoFieldsPresentEveryWhere()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();

        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, DAY1, URL1).add(PARTY_ID, PARTY_ID1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputErrorCount(0);

        // One webSession expected = 2 documents:1 for backup + 1 for current.
        testRunner.assertOutputRecordsCount(2);
        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
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
                                  .sessionInactivityDuration(SESSION_TIMEOUT)
                                  .check(PARTY_ID, PARTY_ID1)
                                  .check(B2BUNIT, null);
    }

    @Test
    public void testUpdateOneWebSessionNow()
            throws Exception
    {
        this.elasticsearchClient.documents.clear();

        Instant firstEvent = Instant.now().minusSeconds(20);
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        Instant lastEvent = firstEvent.plusSeconds(2);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, lastEvent.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        lastEvent = lastEvent.plusSeconds(8);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, lastEvent.toEpochMilli(), URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        // One webSession expected.
        Assert.assertEquals(2, this.elasticsearchClient.documents.size());
        testRunner.assertOutputRecordsCount(2);
        Set<String> ids = this.elasticsearchClient.documents.keySet().stream().map(id->id.split("/")[2]).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));
        Assert.assertTrue(ids.contains(IncrementalWebSession.toDocumentId(SESSION1, LocalDate.now())));

        final MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(firstEvent.toEpochMilli())
                                  .h2kTimestamp(firstEvent.toEpochMilli())
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(3)
                                  .lastEventDateTime(lastEvent.toEpochMilli())
                                  .lastVisitedPage(URL3)
                                  .sessionDuration(Duration.between(firstEvent, lastEvent).getSeconds())
                                  .is_sessionActive(true)
                                  .sessionInactivityDuration(null);


        testRunner.assertOutputErrorCount(0);
    }

    @Test
    public void testUpdateOneWebSessionTimedout()
        throws Exception
    {
        this.elasticsearchClient.documents.clear();

        // Create a web session with timestamp 2s before timeout.
        Instant firstEvent = Instant.now().minusSeconds(SESSION_TIMEOUT-2);
        TestRunner testRunner = newTestRunner();
        testRunner.assertValid();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, firstEvent.toEpochMilli(), URL1)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        MockRecord doc = getRecord(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(doc).lastVisitedPage(URL1);

        // Update web session with timestamp 1s before timeout.
        Instant event = firstEvent.plusSeconds(1);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, event.toEpochMilli(), URL2)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        doc = getRecord(SESSION1, testRunner.getOutputRecords());
        new WebSessionChecker(doc).lastVisitedPage(URL2);

        Thread.sleep(5000); // Make sure the Instant.now performed in the processor will exceed timeout.

        // Update web session with NOW+2s+SESSION_TIMEOUT.
        Instant lastEvent = event.plusSeconds(1);
        testRunner = newTestRunner();
        testRunner.enqueue(Arrays.asList(new WebEvent(SESSION1, USER1, lastEvent.toEpochMilli(), URL3)));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.getOutputRecords().forEach(record -> this.elasticsearchClient.save(record));

        // One webSession expected.
        Assert.assertEquals(2, this.elasticsearchClient.documents.size());
        Set<String> ids = this.elasticsearchClient.documents.keySet().stream().map(id->id.split("/")[2]).collect(Collectors.toSet());
        Assert.assertTrue(ids.contains(SESSION1));
        Assert.assertTrue(ids.contains(IncrementalWebSession.toDocumentId(SESSION1, LocalDate.now())));

        doc = getRecord(SESSION1, testRunner.getOutputRecords());

        new WebSessionChecker(doc).sessionId(SESSION1)
                                  .Userid(USER1)
                                  .record_type("consolidate-session")
                                  .record_id(SESSION1)
                                  .firstEventDateTime(firstEvent.toEpochMilli())
                                  .h2kTimestamp(firstEvent.toEpochMilli())
                                  .firstVisitedPage(URL1)
                                  .eventsCounter(3)
                                  .lastEventDateTime(lastEvent.toEpochMilli())
                                  .lastVisitedPage(URL3)
                                  .sessionDuration(Duration.between(firstEvent, lastEvent).getSeconds())
                                  .is_sessionActive(false)
                                  .sessionInactivityDuration(SESSION_TIMEOUT);


        testRunner.assertOutputRecordsCount(2);
        testRunner.assertOutputErrorCount(0);
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
        final TestRunner runner = TestRunners.newTestRunner(IncrementalWebSession.class);

        runner.addControllerService("elasticsearchClient", elasticsearchClient);
        runner.enableControllerService(elasticsearchClient);
        runner.setProperty(setSourceOfTraffic.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");

        runner.setProperty(IncrementalWebSession.ES_INDEX_FIELD, INDEX);
        runner.setProperty(IncrementalWebSession.ES_TYPE_FIELD, TYPE);
        runner.setProperty(IncrementalWebSession.ES_BACKUP_INDEX_FIELD, INDEX+"-backup");

        runner.setProperty(IncrementalWebSession.SESSION_ID_FIELD, SESSION_ID);
        runner.setProperty(IncrementalWebSession.TIMESTAMP_FIELD, TIMESTAMP);
        runner.setProperty(IncrementalWebSession.VISITED_PAGE_FIELD, VISITED_PAGE);
        runner.setProperty(IncrementalWebSession.USER_ID_FIELD, USER_ID);

        runner.setProperty(IncrementalWebSession.FIELDS_TO_RETURN, FIELDS_TO_RETURN);
        return runner;
    }

    /**
     * The class represents a web event.
     */
    private static class WebEvent extends StandardRecord
    {
        /**
         * Creates a new instance of this class with the provided parameter.
         *
         * @param sessionId the session identifier.
         * @param userId the user identifier.
         * @param timestamp the h2kTimestamp.
         * @param url the visited address.
         */
        public WebEvent(final String sessionId, final String userId, final Long timestamp, final String url)
        {
            this.setField(SESSION_ID, FieldType.STRING, sessionId)
                .setField(USER_ID, FieldType.STRING, userId)
                .setField(TIMESTAMP, FieldType.STRING, timestamp)
                .setField(VISITED_PAGE, FieldType.STRING, url);
        }

        public WebEvent add(final String name, final String value)
        {
            this.setStringField(name, value);
            return this;
        }
    }

    /**
     * A class for testing web session.
     */
    private static class WebSessionChecker
    {
        private final Record record;

        /**
         * Creates a new instance of this class with the provided parameter.
         *
         * @param record the fields to check.
         */
        public WebSessionChecker(final MockRecord record)
        {
            this.record = record;
        }

        public WebSessionChecker sessionId(final Object value) { return check("sessionId", value); }
        public WebSessionChecker Userid(final Object value) { return check("Userid", value); }
        public WebSessionChecker record_type(final Object value) { return check("record_type", value); }
        public WebSessionChecker record_id(final Object value) { return check("record_id", value); }
        public WebSessionChecker firstEventDateTime(final long value) { return check("firstEventDateTime", new Date(value).toString()); }
        public WebSessionChecker h2kTimestamp(final long value) { return check("h2kTimestamp", value); }
        public WebSessionChecker firstVisitedPage(final Object value) { return check("firstVisitedPage", value); }
        public WebSessionChecker eventsCounter(final long value) { return check("eventsCounter", value); }
        public WebSessionChecker lastEventDateTime(final long value) { return check("lastEventDateTime", new Date(value).toString()); }
        public WebSessionChecker lastVisitedPage(final Object value) { return check("lastVisitedPage", value); }
        public WebSessionChecker sessionDuration(final Object value) { return check("sessionDuration", value); }
        public WebSessionChecker is_sessionActive(final Object value) { return check("is_sessionActive", value); }
        public WebSessionChecker sessionInactivityDuration(final Object value) { return check("sessionInactivityDuration", value); }
        public WebSessionChecker record_time(final Object value) { return check("record_time", value); }

        /**
         * Checks the value associated to the specified name against the provided expected value.
         * An exception is thrown if the check fails.
         *
         * @param name the name of the field to check.
         * @param expectedValue the expected value.
         *
         * @return this object for convenience.
         */
        public WebSessionChecker check(final String name, final Object expectedValue)
        {
            final Field field = this.record.getField(name);
            Assert.assertEquals(expectedValue==null?null:expectedValue,
                                field!=null?field.getRawValue():null);
            return this;
        }
    }

    /**
     * A test implementation of ElasticsearchClientService that performs read/write in a map.
     */
    private static final class ESC
                         extends AbstractControllerService
                         implements ElasticsearchClientService
    {
        /**
         * A map that stores elasticsearch documents as sourceAsMap.
         */
        private final Map<String/*toId*/, Map<String, String>/*sourceAsMap*/> documents = new HashMap<>();

        /**
         * Returns the concatenation of provided parameters as docIndex/docType/optionalId.
         *
         * @param docIndex the elasticsearch index
         * @param docType the elasticsearch type
         * @param optionalId the elasticsearch document identifier.
         *
         * @return the concatenation of provided parameters as docIndex/docType/optionalId.
         */
        private static String toId(String docIndex, String docType, String optionalId)
        {
            return docIndex + "/" + docType + "/" + optionalId;
        }

        private void save(final Record record)
        {
            final String sessionId = record.getId();
            final String index = sessionId.contains("-")?INDEX_BACKUP:INDEX;
            final Map<String, String> sourceAsMap = record.getFieldsEntrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(),
                                                            entry.getValue()==null?
                                                                    null:entry.getValue().getRawValue().toString()))
                .collect(Collectors.toMap(Map.Entry::getKey,
                                          Map.Entry::getValue));
            this.documents.put(toId(index, TYPE, sessionId), sourceAsMap);
        }

        @Override
        public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords)
        {
            final List<MultiGetResponseRecord> result = new ArrayList<>();

            for(final MultiGetQueryRecord request: multiGetQueryRecords)
            {
                for(final String id: request.getDocumentIds())
                {
                    final String index = request.getIndexName();
                    final String type = request.getTypeName();
                    final String _id = toId(index, type, id);
                    Map<String, String> document = documents.get(_id);
                    if ( document != null )
                    {
                        result.add(new MultiGetResponseRecord(index, type, id, document));
                    }
//                    System.out.println("Requested document _id="+_id+" present="+(document!=null));
                }
            }

            return result;
        }

        @Override
        public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> optionalId)
        {
            optionalId.orElseThrow(IllegalArgumentException::new);

            final Map<String, String> map = document.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(),
                                                            entry.getValue()==null?null:entry.getValue().toString()))
                .collect(Collectors.toMap(Map.Entry::getKey,
                                          Map.Entry::getValue));

            documents.put(toId(docIndex, docType, optionalId.get()), map);
        }

        @Override
        public void flushBulkProcessor() {}

        @Override
        public boolean existsIndex(String indexName) throws IOException { return false; }

        @Override
        public void refreshIndex(String indexName) throws Exception {}

        @Override
        public void saveAsync(String indexName, String doctype, Map<String, Object> doc) throws Exception {}

        @Override
        public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {}

        @Override
        public long countIndex(String indexName) throws Exception { return 0; }

        @Override
        public void createIndex(int numShards, int numReplicas, String indexName) throws IOException {}

        @Override
        public void dropIndex(String indexName) throws IOException {}

        @Override
        public void copyIndex(String reindexScrollTimeout, String srcIndex, String dstIndex) throws IOException {}

        @Override
        public void createAlias(String indexName, String aliasName) throws IOException {}

        @Override
        public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws IOException { return false; }

        @Override
        public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue) { return 0; }

        @Override
        public String convertRecordToString(Record record) { return ElasticsearchRecordConverter.convertToString(record);}

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors(){ return Collections.emptyList(); }
    }
}
