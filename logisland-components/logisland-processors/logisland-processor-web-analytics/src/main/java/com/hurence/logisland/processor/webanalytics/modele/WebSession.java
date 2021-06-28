package com.hurence.logisland.processor.webanalytics.modele;

import com.hurence.logisland.processor.webanalytics.IncrementalWebSession;
import com.hurence.logisland.processor.webanalytics.util.DateUtils;
import com.hurence.logisland.processor.webanalytics.util.SessionsCalculator;
import com.hurence.logisland.record.*;

import java.time.*;
import java.util.Collection;
import java.util.Map;

import static com.hurence.logisland.processor.webanalytics.util.Utils.isFieldAssigned;

/**
 * This class represents a session which can be created from a given web-event or an existing session
 * represented by a record.
 */
public class WebSession
        extends RecordItem
        implements Comparable<WebSession> {


    private final InternalFields fieldsNames;
    /**
     * Creates a new instance of this class with:
     * - the session identifier set from the web event's session identifier
     * - the first and last timestamps set from the web event's timestamp.
     *
     * @param webEvent the web event to fetch information from.
     */
    public static WebSession fromWebEvent(final Event webEvent,
                      InternalFields fieldsNames,
                      Collection<String> fieldsToCopyToWebSession) {
        WebSession webSession = fromWebEvent(webEvent, fieldsNames);
        if ((fieldsToCopyToWebSession != null) && (!fieldsToCopyToWebSession.isEmpty())) {
            for (final String fieldnameToAdd : fieldsToCopyToWebSession) {
                final Field field = webEvent.record.getField(fieldnameToAdd);
                if (isFieldAssigned(field)) {
                    webSession.getRecord().setField(field);
                }
            }
        }
        return webSession;
    }
    /**
     * Creates a new instance of this class with:
     * - the session identifier set from the web event's session identifier
     * - the first and last timestamps set from the web event's timestamp.
     *
     * @param webEvent the web event to fetch information from.
     */
    public static WebSession fromWebEvent(final Event webEvent,
                                          InternalFields fieldsNames) {

        StandardRecord record = new StandardRecord(IncrementalWebSession.OUTPUT_RECORD_TYPE);
        record.setId(webEvent.getSessionId());
        record.setField(fieldsNames.sessionIdField, FieldType.STRING, webEvent.getSessionId());
        final long eventTimestamp = webEvent.getEpochTimeStampMilli();

        WebSession webSession = new WebSession(record, fieldsNames);
        webSession.setFirstEvent(eventTimestamp);
        webSession.setLastEvent(eventTimestamp);
        webSession.setIsSinglePageVisit(true);
        return webSession;
    }



    /**
     * Returns a new WebSession based on the specified map that represents a web session in elasticsearch.
     *
     * @param sourceAsMap the web session stored in elasticsearch.
     * @param recordType the recordType value for record.
     * @return a new WebSession based on the specified map that represents a web session in elasticsearch.
     */
    public static WebSession fromMap(final Map<String, Object> sourceAsMap,
                                     InternalFields sessionInternalFields,
                                     String recordType) {
        final Record record = new StandardRecord(recordType);
        sourceAsMap.forEach((key, value) ->
        {
                record.setField(key, FieldType.STRING, value);
        });
        record.setId(record.getField(sessionInternalFields.getSessionIdField()).asString());
        return new WebSession(record, sessionInternalFields);
    }


    /**
     * Creates a new instance of this class that wraps the provided record.
     *
     * @param recordRepresentingSession the embedded record.
     */
    public WebSession(final Record recordRepresentingSession,
                      InternalFields fieldsNames) {
        super(recordRepresentingSession);
        this.fieldsNames = fieldsNames;
        record.getAllFields().forEach(field -> {
            String key = field.getName();
            String value = field.asString();
            if (value != null) {
                if (fieldsNames.getIsSessionActiveField().equals(key)
                 || fieldsNames.getIsSinglePageVisit().equals(key)) {
                    record.setField(key, FieldType.BOOLEAN, Boolean.valueOf(value));
                } else if (fieldsNames.getSessionDurationField().equals(key)
                        || fieldsNames.getEventsCounterField().equals(key)
                        || fieldsNames.getTimestampField().equals(key)
                        || fieldsNames.getSessionInactivityDurationField().equals(key)
                        || fieldsNames.getFirstEventEpochSecondsField().equals(key)
                        || fieldsNames.getLastEventEpochSecondsField().equals(key)
                        || FieldDictionary.RECORD_TIME.equals(key)) {
                    record.setField(key, FieldType.LONG, Long.valueOf(value));
                } else {
                    record.setField(key, FieldType.STRING, value);
                }
            }
        });
    }

    public String getSessionId() {
        return this.getStringValue(fieldsNames.sessionIdField);
    }

    public String getOriginalSessionId() {
        String sessionsId = getSessionId();
        return SessionsCalculator.extractOrignalSessionsId(sessionsId);
    }

    /**
     * Returns {@code true} if the specified timestamp is enclosed within the first and last timestamp of this
     * session; {@code false} otherwise.
     *
     * @param timestamp the timestamp to check against this session.
     * @return {@code true} if the specified timestamp is enclosed within the first and last timestamp of this
     * session; {@code false} otherwise.
     */
    public boolean containsTimestamp(final ZonedDateTime timestamp) {
        return this.getFirstEvent().compareTo(timestamp) <= 0 && timestamp.compareTo(this.getLastEvent()) <= 0;
    }

    public boolean timestampFromPast(final ZonedDateTime timestamp) {
        return timestamp.compareTo(this.getLastEvent()) < 0;
    }

    @Override
    public int compareTo(final WebSession session) {
        if (this.getLastEvent().compareTo(session.getFirstEvent()) < 0) {
            return -1;
        } else if (session.getLastEvent().compareTo(this.getFirstEvent()) < 0) {
            return 1;
        } else {
            throw new IllegalStateException("Two sessions can no share same timestamp:" + this.toString()
                    + " vs " + session.toString());
        }
    }

    public ZonedDateTime getFirstEvent() {
        return fromEpochSecond(getFirstEventEpochSeconds());
    }

    public long getFirstEventEpochSeconds() {
        final Field field = record.getField(fieldsNames.firstEventEpochSecondsField);
        if (field == null) {
            // Fallback by parsing the equivalent human readable field.
            return DateUtils.toEpochSecond(record.getField(fieldsNames.firstEventDateTimeField).asString());
        }
        return field.asLong();
    }

    public ZonedDateTime getLastEvent() {
        return fromEpochSecond(getLastEventEpochSeconds());
    }

    public long getLastEventEpochSeconds() {
        final Field field = record.getField(fieldsNames.lastEventEpochSecondsField);
        if (field == null) {
            // Fallback by parsing the equivalent human readable field.
            return DateUtils.toEpochSecond(record.getField(fieldsNames.lastEventDateTimeField).asString());
        }
        return field.asLong();
    }

    public String getSourceOfTraffic() {
        return concatFieldsOfTraffic((String) this.getValue(fieldsNames.sourceOffTrafficSourceField),
                (String) this.getValue(fieldsNames.sourceOffTrafficMediumField),
                (String) this.getValue(fieldsNames.sourceOffTrafficCampaignField),
                (String) this.getValue(fieldsNames.sourceOffTrafficKeyWordField),
                (String) this.getValue(fieldsNames.sourceOffTrafficContentField));
    }

    @Override
    public String toString() {
        return "WebSession{" + record.getField(fieldsNames.firstEventDateTimeField).asString() +
                "-" + record.getField(fieldsNames.lastEventDateTimeField).asString() + "}";
    }

    public void setFirstEvent(final long eventTimestamp) {
        this.record.setField(fieldsNames.firstEventDateTimeField, FieldType.STRING, DateUtils.toFormattedDate(eventTimestamp));
        this.record.setField(fieldsNames.firstEventEpochSecondsField, FieldType.LONG, eventTimestamp / 1000);
    }

    public void setLastEvent(final long eventTimestamp) {
        this.record.setField(fieldsNames.lastEventDateTimeField, FieldType.STRING, DateUtils.toFormattedDate(eventTimestamp));
        this.record.setField(fieldsNames.lastEventEpochSecondsField, FieldType.LONG, eventTimestamp / 1000);
    }

    public void setIsSinglePageVisit(final Boolean isSinglePageVisit) {
        this.record.setField(fieldsNames.isSinglePageVisit, FieldType.BOOLEAN, isSinglePageVisit);
    }

    public static class InternalFields {
        private String timestampField;
        private String sessionIdField;
        private String sourceOffTrafficSourceField;
        private String sourceOffTrafficMediumField;
        private String sourceOffTrafficCampaignField;
        private String sourceOffTrafficKeyWordField;
        private String sourceOffTrafficContentField;

        private String eventsCounterField;
        private String firstVisitedPageField;
        private String lastVisitedPageField;
        private String firstEventDateTimeField;
        private String lastEventDateTimeField;

        private String firstEventEpochSecondsField;
        private String lastEventEpochSecondsField;

        private String userIdField;

        private String isSinglePageVisit;
        private String isSessionActiveField;
        private String sessionInactivityDurationField;
        private String sessionDurationField;

        private String transactionIdsField;

        public InternalFields() { }

        public String getTimestampField() {
            return timestampField;
        }

        public WebSession.InternalFields setTimestampField(String timestampField) {
            this.timestampField = timestampField;
            return this;
        }

        public String getSessionIdField() {
            return sessionIdField;
        }

        public WebSession.InternalFields setSessionIdField(String sessionIdField) {
            this.sessionIdField = sessionIdField;
            return this;
        }

        public String getSourceOffTrafficSourceField() {
            return sourceOffTrafficSourceField;
        }

        public WebSession.InternalFields setSourceOffTrafficSourceField(String sourceOffTrafficSourceField) {
            this.sourceOffTrafficSourceField = sourceOffTrafficSourceField;
            return this;
        }

        public String getSourceOffTrafficMediumField() {
            return sourceOffTrafficMediumField;
        }

        public WebSession.InternalFields setSourceOffTrafficMediumField(String sourceOffTrafficMediumField) {
            this.sourceOffTrafficMediumField = sourceOffTrafficMediumField;
            return this;
        }

        public String getSourceOffTrafficCampaignField() {
            return sourceOffTrafficCampaignField;
        }

        public WebSession.InternalFields setSourceOffTrafficCampaignField(String sourceOffTrafficCampaignField) {
            this.sourceOffTrafficCampaignField = sourceOffTrafficCampaignField;
            return this;
        }

        public String getSourceOffTrafficKeyWordField() {
            return sourceOffTrafficKeyWordField;
        }

        public WebSession.InternalFields setSourceOffTrafficKeyWordField(String sourceOffTrafficKeyWordField) {
            this.sourceOffTrafficKeyWordField = sourceOffTrafficKeyWordField;
            return this;
        }

        public String getSourceOffTrafficContentField() {
            return sourceOffTrafficContentField;
        }

        public WebSession.InternalFields setSourceOffTrafficContentField(String sourceOffTrafficContentField) {
            this.sourceOffTrafficContentField = sourceOffTrafficContentField;
            return this;
        }

        public String getEventsCounterField() {
            return eventsCounterField;
        }

        public InternalFields setEventsCounterField(String eventsCounterField) {
            this.eventsCounterField = eventsCounterField;
            return this;
        }

        public String getFirstVisitedPageField() {
            return firstVisitedPageField;
        }

        public InternalFields setFirstVisitedPageField(String firstVisitedPageField) {
            this.firstVisitedPageField = firstVisitedPageField;
            return this;
        }

        public String getLastVisitedPageField() {
            return lastVisitedPageField;
        }

        public InternalFields setLastVisitedPageField(String lastVisitedPageField) {
            this.lastVisitedPageField = lastVisitedPageField;
            return this;
        }

        public String getFirstEventDateTimeField() {
            return firstEventDateTimeField;
        }

        public InternalFields setFirstEventDateTimeField(String firstEventDateTimeField) {
            this.firstEventDateTimeField = firstEventDateTimeField;
            return this;
        }

        public String getLastEventDateTimeField() {
            return lastEventDateTimeField;
        }

        public InternalFields setLastEventDateTimeField(String lastEventDateTimeField) {
            this.lastEventDateTimeField = lastEventDateTimeField;
            return this;
        }

        public String getUserIdField() {
            return userIdField;
        }

        public InternalFields setUserIdField(String userIdField) {
            this.userIdField = userIdField;
            return this;
        }

        public String getIsSinglePageVisit() {
            return isSinglePageVisit;
        }

        public InternalFields setIsSinglePageVisit(String isSinglePageVisit) {
            this.isSinglePageVisit = isSinglePageVisit;
            return this;
        }

        public String getIsSessionActiveField() {
            return isSessionActiveField;
        }

        public InternalFields setIsSessionActiveField(String isSessionActiveField) {
            this.isSessionActiveField = isSessionActiveField;
            return this;
        }

        public String getSessionInactivityDurationField() {
            return sessionInactivityDurationField;
        }

        public InternalFields setSessionInactivityDurationField(String sessionInactivityDurationField) {
            this.sessionInactivityDurationField = sessionInactivityDurationField;
            return this;
        }

        public String getSessionDurationField() {
            return sessionDurationField;
        }

        public InternalFields setSessionDurationField(String sessionDurationField) {
            this.sessionDurationField = sessionDurationField;
            return this;
        }

        public String getTransactionIdsField() {
            return transactionIdsField;
        }

        public InternalFields setTransactionIdsField(String transactionIdsField) {
            this.transactionIdsField = transactionIdsField;
            return this;
        }

        public String getFirstEventEpochSecondsField() {
            return firstEventEpochSecondsField;
        }

        public InternalFields setFirstEventEpochSecondsField(String firstEventEpochSecondsField) {
            this.firstEventEpochSecondsField = firstEventEpochSecondsField;
            return this;
        }

        public String getLastEventEpochSecondsField() {
            return lastEventEpochSecondsField;
        }

        public InternalFields setLastEventEpochSecondsField(String lastEventEpochSecondsField) {
            this.lastEventEpochSecondsField = lastEventEpochSecondsField;
            return this;
        }
    }
}
