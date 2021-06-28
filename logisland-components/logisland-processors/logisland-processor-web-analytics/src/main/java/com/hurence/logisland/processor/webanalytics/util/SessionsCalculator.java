package com.hurence.logisland.processor.webanalytics.util;

import com.hurence.logisland.processor.webanalytics.IncrementalWebSession;
import com.hurence.logisland.processor.webanalytics.modele.*;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hurence.logisland.processor.webanalytics.util.Utils.isFieldAssigned;

/**
 * This class represents one or more sessions resulting of the processing of web events.
 */
public class SessionsCalculator {

    private static Logger logger = LoggerFactory.getLogger(SessionsCalculator.class);

    private final String divolteSessionId;
//    private final WebSession lastSessionBeforeProcessing;
    // The resulting sessions from the processed web events.
    // MAKE SURE LAST SESSION IS AT LAST POSITION!!!
    private final List<WebSession> calculatedSessions = new ArrayList<>();
    private final Collection<SessionCheck> checkers;

    private long eventProcessedCounter = 0;
//    private final String _NEW_SESSION_REASON_FIELD;
    private final WebSession.InternalFields webSessionInternalFields;
    private final Event.InternalFields eventInternalFields;
    private final long sessionInactivityTimeoutInSeconds;
    private final Collection<String> fieldsToCopyFromEventsToSessions;


    public static String extractOrignalSessionsId(String sessionId) {
        final String[] splittedSessionId = sessionId.split(IncrementalWebSession.EXTRA_SESSION_DELIMITER);
        return splittedSessionId[0];
    }

    public SessionsCalculator(Collection<SessionCheck> checkers,
                              long sessionInactivityTimeoutInSeconds,
                              WebSession.InternalFields webSessionInternalFields,
                              Event.InternalFields eventInternalFields,
                              Collection<String> fieldsToCopyFromEventsToSessions,
                              String divolteSessionId) {

        this.checkers = checkers;
        this.sessionInactivityTimeoutInSeconds = sessionInactivityTimeoutInSeconds;
        this.webSessionInternalFields = webSessionInternalFields;
        this.eventInternalFields = eventInternalFields;
        this.fieldsToCopyFromEventsToSessions = fieldsToCopyFromEventsToSessions;
        this.divolteSessionId = divolteSessionId;
    }

    /**
     * Returns the session identifier of this session.
     *
     * @return the session identifier of this session.
     */
    public String getDivolteSessionId() {
        return this.divolteSessionId;
    }


    /**
     * Returns the processed web sessions.
     *
     * @return the processed web sessions.
     */
    public Collection<WebSession> getCalculatedSessions() {
        return calculatedSessions;
    }

    /**
     * Returns the last sessionId (#?) of this session container.
     *
     * @return the last sessionId (#?) of this session container.
     */
    public String getLastSessionId() {
        String result = this.divolteSessionId;

        if (!this.calculatedSessions.isEmpty()) {
            result = this.calculatedSessions.get(this.calculatedSessions.size() - 1).getSessionId();
        } else {
            logger.error("Invalid state: session container for '" + this.divolteSessionId + "' is empty. " +
                    "At least one session is expected");
        }

        return result;
    }

    /**
     * Processes the provided events against the first session (if any).
     *
     * @param events the events to process.
     * @return this object for convenience.
     */
    public SessionsCalculator processEventsKnowingLastSession(final Events events, final WebSession currentWebSession) {
        logger.debug("Applying {} events to session '{}'", events.size(), events.getOriginalSessionId());
        if (currentWebSession != null) {
            events.forEach(event -> event.setSessionId(currentWebSession.getSessionId()));
        } else {
            //considered as first session
            events.forEach(event -> event.setSessionId(divolteSessionId));
        }
        this.processEvents(currentWebSession, events);
        return this;
    }

    public SessionsCalculator processEvents(final Events events, final String currentSessionId) {
        logger.debug("Applying {} events to session '{}'", events.size(), events.getOriginalSessionId());
        if (currentSessionId != null) {
            events.forEach(event -> event.setSessionId(currentSessionId));
        } else {
            //considered as first session
            events.forEach(event -> event.setSessionId(divolteSessionId));
        }
        this.processEvents(null, events);
        return this;
    }
    /**
     * Returns {@code true} if the specified web-event checked against the provided web-session is valid;
     * {@code false} otherwise.
     * In case the returned value is {@code false} then a new session must be created.
     *
     * @param webSession the web-session to check the web-event against.
     * @param webEvent   the web-event to validate.
     * @return {@code true} if the specified web-event checked against the provided web-session is valid;
     * {@code false} otherwise.
     */
    private SessionCheckResult isEventApplicable(final WebSession webSession,
                                                 final Event webEvent) {
        SessionCheckResult result = ValidSessionCheckResult.getInstance();
        for (final SessionCheck check : checkers) {
            result = check.isValid(webSession, webEvent);
            if (!result.isValid()) {
                break;
            }
        }

        return result;
    }

    /**
     * Processes the provided events against the provided session (can be {@code null}).
     *
     * @param session the session to update. If {@code null} is provided a new session is created.
     * @param events  the events to process.
     * @return this object for convenience.
     */
    private void processEvents(WebSession session,
                               final Events events) {
        if (events.isEmpty()) {
            // No event. Paranoid.
            return;
        }

        final Iterator<Event> iterator = events.iterator();
        logger.debug("Processing event sessionId=" + events.getOriginalSessionId() + " eventCount=" + eventProcessedCounter);

        if (session == null) {
            // No web-session yet in datastore.
            Event event = iterator.next();
            eventProcessedCounter++;
            session = WebSession.fromWebEvent(event,
                    webSessionInternalFields,
                    fieldsToCopyFromEventsToSessions);
            add(session, event);
        }

        this.calculatedSessions.add(session);

        while (iterator.hasNext()) {
            final Event event = iterator.next();
            eventProcessedCounter++;

            final SessionCheckResult isSessionValid = isEventApplicable(session, event);

            if (isSessionValid.isValid()) {
                // No invalid check found.
                add(session, event);
            } else {
                // Invalid check found:
                // 1. keep current web-session untouched (and save it)
                // 2. create a new web-session from the current web-event and rename/increase session-id.
                final String[] oldSessionIdSplitted = event.getSessionId().split(IncrementalWebSession.EXTRA_SESSION_DELIMITER);
                final int index = (oldSessionIdSplitted.length == 1) ? 2 // only one web session so far => create 2nd one
                        : Integer.valueOf(oldSessionIdSplitted[1]) + 1; // +1 on web session
                final String newSessionId = oldSessionIdSplitted[0] + IncrementalWebSession.EXTRA_SESSION_DELIMITER + index;
                final Collection<Event> eventsForNextSession = events.tailSet(event);
                // Rewrite all remaining web-events with new session identifier.
                eventsForNextSession.forEach(eventToChangeSession -> eventToChangeSession.setSessionId(newSessionId));
                // Mark event that triggered the new sessions with the reason.
                event.getRecord().setField(eventInternalFields.getNewSessionReasonField(), FieldType.STRING, isSessionValid.reason());

                final Events nextEvents = new Events(eventsForNextSession);

                this.processEvents(null/*force new web-session*/, nextEvents);
                break;
            }
        }
    }

    /**
     * Adds the specified event to this sessions by updating fields such as lastVisitedPage.
     *
     * @param event the event to apply.
     */
    private void add(final WebSession session,
                     final Event event) {
        // Handle case where web-event is older that first event of session.
        // In case there are few events older than the current web-session, all those events must
        // be taken into account despite the fact that setting the timestamp of the first event
        // will 'hide' the next ones.
        final Field eventTimestampField = event.getRecord().getField(eventInternalFields.getTimestampField());
        final long eventTimestamp = eventTimestampField.asLong();

        // Sanity check.
        final Field lastEventField = session.getRecord().getField(webSessionInternalFields.getLastEventEpochSecondsField());
        if (lastEventField != null) {
            final long lastEvent = lastEventField.asLong();
            if (lastEvent > 0 && eventTimestamp > 0 && eventTimestamp < lastEvent) {
                // The event is older that current web session; ignore.
                return;
            }
        }

        Record sessionInternalRecord = session.getRecord();
        // EVENTS_COUNTER
        Field field = sessionInternalRecord.getField(webSessionInternalFields.getEventsCounterField());
        long eventsCounter = field == null ? 0 : field.asLong();
        eventsCounter++;
        sessionInternalRecord.setField(webSessionInternalFields.getEventsCounterField(), FieldType.LONG, eventsCounter);

        // TIMESTAMP
        // Set the session create timestamp to the create timestamp of the first event on the session.
        long creationTimestamp;
        field = sessionInternalRecord.getField(webSessionInternalFields.getTimestampField());
        if (!isFieldAssigned(field)) {
            sessionInternalRecord.setField(webSessionInternalFields.getTimestampField(), FieldType.LONG, eventTimestamp);
            creationTimestamp = eventTimestamp;
        } else {
            creationTimestamp = field.asLong();
        }

        final Field visitedPage = event.getRecord().getField(eventInternalFields.getVisitedPageField());
        // FIRST_VISITED_PAGE
        if (!isFieldAssigned(sessionInternalRecord.getField(webSessionInternalFields.getFirstVisitedPageField()))) {
            sessionInternalRecord.setField(webSessionInternalFields.getFirstVisitedPageField(), FieldType.STRING, visitedPage.asString());
        }

        // LAST_VISITED_PAGE
        if (isFieldAssigned(visitedPage)) {
            sessionInternalRecord.setField(webSessionInternalFields.getLastVisitedPageField(), FieldType.STRING, visitedPage.asString());
        }

        // FIRST_EVENT_DATETIME
        if (!isFieldAssigned(sessionInternalRecord.getField(webSessionInternalFields.getFirstEventDateTimeField()))) {
            session.setFirstEvent(eventTimestamp);
        }

        // LAST_EVENT_DATETIME
        if (isFieldAssigned(eventTimestampField)) {
            session.setLastEvent(eventTimestamp);
        }

        // USERID
        // Add the userid sessionInternalRecord if available
        final Field userIdField = sessionInternalRecord.getField(webSessionInternalFields.getUserIdField());
        if ((!isFieldAssigned(userIdField) || "undefined".equalsIgnoreCase(userIdField.asString()))
                && isFieldAssigned(event.getRecord().getField(eventInternalFields.getUserIdField()))) {
            final String userId = event.getRecord().getField(eventInternalFields.getUserIdField()).asString();
            if (userId != null) {
                sessionInternalRecord.setField(webSessionInternalFields.getUserIdField(), FieldType.STRING, userId);
            }
        }

        // IS_SINGLE_PAGE
        final Boolean currentIsSinglePageVisit = sessionInternalRecord.getField(webSessionInternalFields.getIsSinglePageVisit()).asBoolean();
        final String sessionFirstVisitedPage = sessionInternalRecord.getField(webSessionInternalFields.getFirstVisitedPageField()).asString();
        session.setIsSinglePageVisit(currentIsSinglePageVisit && sessionFirstVisitedPage.equals(visitedPage.asString()));

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime eventLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventTimestamp),
                ZoneId.systemDefault());

        // Compute session inactivity duration (in milliseconds)
        final long sessionInactivityDuration = Duration.between(eventLocalDateTime, now).getSeconds();

        if (sessionInactivityDuration > sessionInactivityTimeoutInSeconds) {//it is a value not a field here
            // Mark the session as closed
            sessionInternalRecord.setField(webSessionInternalFields.getIsSessionActiveField(), FieldType.BOOLEAN, Boolean.FALSE);

            // Max out the sessionInactivityDuration - only pertinent in case of topic rewind.
            sessionInternalRecord.setField(webSessionInternalFields.getSessionInactivityDurationField(), FieldType.LONG, sessionInactivityTimeoutInSeconds);
        } else {
            sessionInternalRecord.setField(webSessionInternalFields.getIsSessionActiveField(), FieldType.BOOLEAN, Boolean.TRUE);
        }

        final long sessionDuration = Duration.between(Instant.ofEpochMilli(creationTimestamp),
                Instant.ofEpochMilli(eventTimestamp)).getSeconds();
        if (sessionDuration > 0) {
            sessionInternalRecord.setField(webSessionInternalFields.getSessionDurationField(), FieldType.LONG, sessionDuration);
        }

        // Extra
        final Field transactionIdField = event.getRecord().getField(eventInternalFields.getTransactionIdField());
        if (isFieldAssigned(transactionIdField)
                && (!"undefined".equalsIgnoreCase(transactionIdField.asString()))
                && (!transactionIdField.asString().isEmpty())) {
            final Field transactionIdsField = sessionInternalRecord.getField(eventInternalFields.getTransactionIdsField());
            Collection<String> transactionIds;
            if (!isFieldAssigned(transactionIdField)) {
                transactionIds = (Collection<String>) transactionIdsField.getRawValue();
            } else {
                transactionIds = new ArrayList<>();
                sessionInternalRecord.setField(webSessionInternalFields.getTransactionIdsField(), FieldType.ARRAY, transactionIds);
            }
            transactionIds.add(transactionIdField.asString());
        }

        if (!sessionInternalRecord.isValid()) {
            sessionInternalRecord.getFieldsEntrySet().forEach(entry ->
            {
                final Field f = entry.getValue();
                logger.debug("INVALID field type={}, class={}", f.getType(), f.getRawValue().getClass());
            });
        }
    }

}
