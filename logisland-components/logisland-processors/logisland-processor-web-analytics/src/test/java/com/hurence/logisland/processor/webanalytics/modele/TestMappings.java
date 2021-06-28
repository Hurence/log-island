package com.hurence.logisland.processor.webanalytics.modele;

import com.hurence.logisland.processor.webanalytics.IncrementalWebSession;

import static com.hurence.logisland.processor.webanalytics.IncrementalWebSession.DEFAULT_SOURCE_OF_TRAFFIC_PREFIX;

public class TestMappings {


    public static Event.InternalFields eventsInternalFields = new Event.InternalFields()
            .setSessionIdField("sessionId")
            .setTimestampField("h2kTimestamp")
            .setVisitedPageField("VISITED_PAGE")
            .setSourceOffTrafficCampaignField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN)
            .setSourceOffTrafficContentField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CONTENT)
            .setSourceOffTrafficKeyWordField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_KEYWORD)
            .setSourceOffTrafficMediumField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_MEDIUM)
            .setSourceOffTrafficSourceField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_SOURCE)
            .setNewSessionReasonField("reasonForNewSession")
            .setUserIdField("Userid")
            .setOriginalSessionIdField("originalSessionId")
            .setTransactionIdField("transactionId")
            .setTransactionIdsField("transactionIds");


    public static WebSession.InternalFields sessionInternalFields = new WebSession.InternalFields()
            .setSessionIdField("sessionId")
                .setTimestampField("h2kTimestamp")
                .setSourceOffTrafficCampaignField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN)
                .setSourceOffTrafficContentField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_CONTENT)
                .setSourceOffTrafficKeyWordField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_KEYWORD)
                .setSourceOffTrafficMediumField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_MEDIUM)
                .setSourceOffTrafficSourceField(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX + IncrementalWebSession.SOURCE_OF_TRAFFIC_FIELD_SOURCE)
                .setIsSessionActiveField("is_sessionActive")
                .setSessionDurationField("sessionDuration")
                .setSessionInactivityDurationField("sessionInactivityDuration")
                .setEventsCounterField("eventsCounter")
                .setFirstEventDateTimeField("firstEventDateTime")
                .setFirstEventEpochSecondsField("firstEventEpochSeconds")
                .setFirstVisitedPageField("firstVisitedPage")
                .setLastEventDateTimeField("lastEventDateTime")
                .setLastEventEpochSecondsField("lastEventEpochSeconds")
                .setLastVisitedPageField("lastVisitedPage")
                .setTransactionIdsField("transactionIds")
                .setUserIdField("Userid")
                .setIsSinglePageVisit("isSinglePageVisit");
}
