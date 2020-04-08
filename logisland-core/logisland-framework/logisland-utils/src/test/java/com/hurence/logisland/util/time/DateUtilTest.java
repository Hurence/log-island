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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.util.time;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.Assert.*;

/**
 * @author tom
 */
public class DateUtilTest {

    private static final TimeZone tz = TimeZone.getTimeZone("Europe/Paris");

    private static Logger logger = LoggerFactory.getLogger(DateUtilTest.class);

    // TODO fix test here on unix

    /**
     * org.junit.ComparisonFailure: expected:<2012-10-19T[18]:12:49.000 CEST> but was:<2012-10-19T[20]:12:49.000 CEST>
     * at org.junit.Assert.assertEquals(Assert.java:115)
     * at org.junit.Assert.assertEquals(Assert.java:144)
     * at com.hurence.logisland.utils.time.DateUtilsTest.testDateToString(DateUtilsTest.java:33)
     *
     * @throws ParseException
     */
    //@Test
    public void testDateToString() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("fr", "FR"));
        Date date = sdf.parse("19/Oct/2012:18:12:49");
        String result = DateUtil.toString(date);
        String expectedResult = "2012-10-19T18:12:49.000 CEST";

        assertEquals(expectedResult, result);
    }

    @Test
    public void testStringToDate() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("en", "US"));
        sdf.setTimeZone(tz);
        Date expectedResult = sdf.parse("19/Oct/2012:18:12:49");
        String dateString = "2012-10-19T18:12:49.000 CEST";
        Date result = DateUtil.fromIsoStringToDate(dateString);
        assertEquals(expectedResult, result);

        Date now = new Date();
        assertEquals(now, DateUtil.fromIsoStringToDate(DateUtil.toString(now)));
    }

    @Test
    public void testIsWeekEnd() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", new Locale("en", "US"));
        Date date = sdf.parse("27/Jul/2013:18:12:49");
        assertTrue(DateUtil.isWeekend(date));
        date = sdf.parse("28/Jul/2013:18:12:49");
        assertTrue(DateUtil.isWeekend(date));
        date = sdf.parse("29/Jul/2013:00:00:01");
        assertFalse(DateUtil.isWeekend(date));
        date = sdf.parse("26/Jul/2013:23:59:59");
        assertFalse(DateUtil.isWeekend(date));
    }

    @Test
    public void testIsWithinRange() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", new Locale("en", "US"));
        Date date = sdf.parse("27/Jul/2013:18:12:49");
        assertTrue(DateUtil.isWithinHourRange(date, 9, 19));
        assertFalse(DateUtil.isWithinHourRange(date, 19, 20));
    }

    @Test
    public void testTimeout() throws InterruptedException {

        Date date = new Date();

        // go in the past from 5"
        date.setTime(date.getTime() - 2000);

        assertFalse(DateUtil.isTimeoutExpired(date, 3000));

        Thread.sleep(2000);
        assertTrue(DateUtil.isTimeoutExpired(date, 3000));
    }


    @Test
    public void testLegacyFormat() throws ParseException {
        String strDate = "Thu Jan 02 08:43:49 CET 2014";
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy", new Locale("en", "US"));
        Date date = sdf.parse(strDate);
        assertTrue(1388648629000L == date.getTime());

        date = DateUtil.fromLegacyStringToDate(strDate);
        assertTrue(1388648629000L == date.getTime());
    }

    @Test
    public void testGetLatestDaysFromNow() throws ParseException {
        String result = DateUtil.getLatestDaysFromNow(3);
        //  String expectedResult = "2012-10-19T18:12:49.000 CEST";

        // assertEquals(expectedResult, result);
    }

    @Test
    public void testParsing() throws ParseException {
        // Date expectedDate = new Date(1388648629000L);
        DateTime today = new DateTime(DateTimeZone.UTC);
        String currentYear = today.year().getAsString();


        DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
        DateTime expectedDate = f.parseDateTime(currentYear + "-01-02 07:43:49");

        String currentDay = expectedDate.dayOfWeek().getAsShortText(Locale.ENGLISH);

        String[] strDates = {
                currentDay + " Jan 02 08:43:49 CET " + currentYear,
                currentDay + ", 02 Jan " + currentYear + " 08:43:49 CET",
                currentYear + "-01-02T08:43:49CET",
                currentYear + "-01-02T08:43:49.000CET",
                currentYear + "-01-02T08:43:49.0000+01:00",
                currentYear + "-01-02 07:43:49",
                currentYear + "-01-02 07:43:49,000",
                currentYear + "-01-02 07:43:49.000",
                "02/JAN/" + currentYear + ":09:43:49 +0200",
                "Jan 02 07:43:49",
                "02/01/" + currentYear + "-07:43:49:000",
                "02-Jan-" + currentYear + " 07:43:49:000",
                "02-Jan-" + currentYear + " 07:43:49.000",
                "02/Jan/" + currentYear + ":03:43:49 -0400",
                currentYear + " Jan 02 07:43:49",
                currentYear + " Jan 2 7:43:49",
                "02/01/" + currentYear + " 07:43:49"
        };


        for (String strDate : strDates) {
            logger.info("parsing " + strDate);
            Date date = DateUtil.parse(strDate);
            assertTrue(date + " should be equal to " + expectedDate.toDate(), expectedDate.getMillis() == date.getTime());

        }

    }

    @Test
    public void testParsing2() throws ParseException {
        DateTime today = new DateTime(DateTimeZone.UTC);
        String currentYear = today.year().getAsString();

        DateTimeFormatter f = DateTimeFormat.forPattern("yyyy").withZone(DateTimeZone.UTC);
        DateTime expectedDate = f.parseDateTime(currentYear);

        Date date = DateUtil.parse(currentYear);
        assertTrue(date + " should be equal to " + expectedDate.toDate(), expectedDate.getMillis() == date.getTime());
    }

    @Test
    public void testParsingWithTimeZone() throws ParseException {

        /**
         * WARNING ! Oracle is playing with timeZones object between minor version changes...
         * For exemple that caused this test to fail with "America/Cancun" timeZone if the jvm
         * version was inferior to 8u45... So I replaced it by "Canada/Atlantic" that should be more stable
         * and put it as a variable so we can easily change it
         */
        TimeZone testTimeZone = TimeZone.getTimeZone("Canada/Atlantic");
        // Date expectedDate = new Date(1388648629000L);
        DateTime today = new DateTime(DateTimeZone.UTC);
        String currentYear = today.year().getAsString();

        DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.forTimeZone(tz));
        DateTime expectedEuropeDate = f.parseDateTime(currentYear + "-01-02 07:43:49");

        Date dateAsEuropeanDate = DateUtil.parse(currentYear + "-01-02 07:43:49", "yyyy-MM-dd HH:mm:ss", tz);


        assertTrue(dateAsEuropeanDate + " should be equal to " + expectedEuropeDate.toDate(),
               dateAsEuropeanDate.getTime() == expectedEuropeDate.getMillis());

        Date dateAsUtcDate = DateUtil.parse(currentYear + "-01-02 07:43:49", "yyyy-MM-dd HH:mm:ss");

        assertTrue(dateAsUtcDate + " should be equal to " + expectedEuropeDate.toDate() + " plus 1 hour",
                dateAsUtcDate.getTime() == expectedEuropeDate.getMillis() + 1000 * 60 * 60);

        Date dateAsTest = DateUtil.parse(currentYear + "-01-02 07:43:49", "yyyy-MM-dd HH:mm:ss", testTimeZone);

        assertTrue(dateAsTest + " should be equal to " + expectedEuropeDate.toDate() + " plus 6 hour",
                dateAsTest.getTime() == expectedEuropeDate.getMillis() + 1000 * 60 * 60 * 5 );

        /**
         * should not use set timezone when timezone on pattern
         */
        f = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(DateTimeZone.forTimeZone(tz));
        DateTime expectedDate = f.parseDateTime("2001-07-04T12:08:56.235-0700");

        dateAsEuropeanDate = DateUtil.parse("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", tz);


        assertTrue(dateAsEuropeanDate + " should be equal to " + expectedDate.toDate(),
                dateAsEuropeanDate.getTime() == expectedDate.getMillis());

        dateAsUtcDate = DateUtil.parse("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        assertTrue(dateAsUtcDate + " should be equal to " + expectedDate.toDate(),
                dateAsUtcDate.getTime() == expectedDate.getMillis());

        dateAsTest = DateUtil.parse("2001-07-04T12:08:56.235-0700", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", testTimeZone);

        assertTrue(dateAsTest + " should be equal to " + expectedDate.toDate(),
                dateAsTest.getTime() == expectedDate.getMillis());

    }
    @Test
    public void testParsingWithoutTimeZone() throws ParseException {
        // Date expectedDate = new Date(1388648629000L);
        DateTime today = new DateTime(DateTimeZone.UTC);
        String currentYear = today.year().getAsString();

        DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
        DateTime expectedUtcDate = f.parseDateTime(currentYear + "-01-02 07:43:49");

        Date dateAsUtcDate = DateUtil.parse(currentYear + "-01-02 07:43:49", "yyyy-MM-dd HH:mm:ss");

        assertTrue(dateAsUtcDate + " should be equal to " + expectedUtcDate.toDate(), expectedUtcDate.getMillis() == dateAsUtcDate.getTime());

    }


    @Test
    public void testStandardDate() throws ParseException {
        final long expectedTimestamp = 1477895624866L;
        final String testedDate = "2016-10-31T06:33:44.866Z";
        final String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        Date date = DateUtil.parse(testedDate, format);;
        logger.info("date :" +date.toString());
        assertEquals(expectedTimestamp, date.getTime());

    }




}
