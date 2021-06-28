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

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.processor.webanalytics.modele.*;
import com.hurence.logisland.processor.webanalytics.util.SimpleSessionsCalculator;
import com.hurence.logisland.processor.webanalytics.util.Utils;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.validator.StandardValidators;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webanalytics.util.Utils.isFieldAssigned;

@Category(ComponentCategory.ANALYTICS)
@Tags({"analytics", "web", "session"})
@CapabilityDescription(
value = "This processor creates web-sessions based on incoming web-events.\n" +

        " Firstly, web-events are grouped by their session identifier and processed in chronological order.\n" +
        " The following fields of the newly created web session are set based on the associated web event:" +
        " session identifier, first timestamp, first visited page." +

        " Secondly, once created, the web session is updated by the remaining web-events.\n" +
        " Updates have impacts on fields of the web session such as event counter, last visited page, " +
        " session duration, ...\n" +
        " Before updates are actually applied, checks are performed to detect rules that would trigger the creation" +
        " of a new session:\n\n" +
        "\tthe duration between the web session and the web event must not exceed the specified time-out,\n" +
        "\tthe web session and the web event must have timestamps within the same day (at midnight a new web session " +
        "is created),\n" +
        "\tsource of traffic (campaign, ...) must be the same on the web session and the web event.\n" +
        "\n" +
        " When a breaking rule is detected, a new web session is created with a new session identifier where as" +
        " remaining web-events still have the original session identifier. The new session identifier is the original" +
        " session suffixed with the character '#' followed with an incremented counter. This new session identifier" +
        " is also set on the remaining web-events.\n" +

        " Finally when all web events were applied, all web events -potentially modified with a new session" +
        " identifier- And web sessions are passed to the next processor.\n" +
        "\n" +
        "WebSession information are:\n" +
        "- first and last visited page\n" +
        "- first and last timestamp of processed event \n" +
        "- total number of processed events\n" +
        "- the userId\n" +
        "- a boolean denoting if the web-session is still active or not\n" +
        "- an integer denoting the duration of the web-sessions\n" +
        "- optional fields that may be retrieved from the processed events\n" +
        "\n"
)
@ExtraDetailFile("./details/CalculWebSession-Detail.rst")
public class CalculWebSession
       extends AbstractProcessor
{

    /**
     * The extra character added in case a missed new session is detected. In that case the original session identifier
     * is suffixes with that special character and the next session number.
     * Eg id-session, id-session#2, id-session#3, ...
     */
    public static final String EXTRA_SESSION_DELIMITER = "#";

    /**
     * The type of the output record.
     */
    public static final String OUTPUT_RECORD_TYPE = "consolidate-session";

    //Session Elasticsearch indices
    public static final String PROP_ES_SESSION_INDEX_PREFIX = "es.session.index.prefix";
    public static final String PROP_ES_SESSION_INDEX_SUFFIX_FORMATTER = "es.session.index.suffix.date";
    public static final String PROP_ES_SESSION_TYPE_NAME = "es.session.type.name";
    //Event Elasticsearch indices
    public static final String PROP_ES_EVENT_INDEX_PREFIX = "es.event.index.prefix";
    public static final String PROP_ES_EVENT_INDEX_SUFFIX_FORMATTER = "es.event.index.suffix.date";
    public static final String PROP_ES_EVENT_TYPE_NAME = "es.event.type.name";

    public static final String PROP_ES_INDEX_SUFFIX_TIMEZONE = "es.index.suffix.timezone";
    /**
     * Extra fields - for convenience - avoiding to parse the human readable first and last timestamps.
     */
    public static final String _FIRST_EVENT_EPOCH_SECONDS_FIELD = "firstEventEpochSeconds";
    public static final String _LAST_EVENT_EPOCH_SECONDS_FIELD = "lastEventEpochSeconds";

    public static final PropertyDescriptor DEBUG_CONF =
            new PropertyDescriptor.Builder()
                    .name("debug")
                    .description("Enable debug. If enabled, debug information are logged.")
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                    .required(false)
                    .defaultValue("false")
                    .build();

    public static final PropertyDescriptor ES_SESSION_INDEX_PREFIX_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_INDEX_PREFIX)
                    .description("Prefix of the indices containing the web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_INDEX_SUFFIX_FORMATTER)
                    .description("suffix to add to prefix for web session indices. It should be valid date format [yyyy.MM].")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .addValidator(StandardValidators.DATE_TIME_FORMATTER_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_SESSION_TYPE_NAME_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_SESSION_TYPE_NAME)
                    .description("Name of the ES type of web session documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor SESSION_INACTIVITY_TIMEOUT_CONF =
            new PropertyDescriptor.Builder()
                    .name("session.timeout")
                    .description("session timeout in sec")
                    .required(false)
                    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                    .defaultValue("1800")
                    .build();

    public static final PropertyDescriptor SESSION_ID_FIELD_CONF =
            new PropertyDescriptor.Builder()
                    .name("sessionid.field")
                    .description("the name of the field containing the session id => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionId")
                    .build();

    public static final PropertyDescriptor ES_EVENT_INDEX_PREFIX_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_INDEX_PREFIX)
                    .description("Prefix of the index containing the web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_INDEX_SUFFIX_FORMATTER)
                    .description("suffix to add to prefix for web event indices. It should be valid date format [yyyy.MM].")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .addValidator(StandardValidators.DATE_TIME_FORMATTER_VALIDATOR)
                    .build();

    public static final PropertyDescriptor ES_EVENT_TYPE_NAME_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_EVENT_TYPE_NAME)
                    .description("Name of the ES type of web event documents.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    public static final PropertyDescriptor TIMESTAMP_FIELD_CONF =
            new PropertyDescriptor.Builder()
                    .name("timestamp.field")
                    .description("the name of the field containing the timestamp => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("h2kTimestamp")
                    .build();

    public static final PropertyDescriptor VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("visitedpage.field")
                    .description("the name of the field containing the visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("location")
                    .build();

    public static final PropertyDescriptor USER_ID_FIELD =
            new PropertyDescriptor.Builder()
                    .name("userid.field")
                    .description("the name of the field containing the userId => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("userId")
                    .build();

    public static final PropertyDescriptor FIELDS_TO_RETURN =
            new PropertyDescriptor.Builder()
                    .name("fields.to.return")
                    .description("the list of fields to return")
                    .required(false)
                    .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                    .build();

    public static final PropertyDescriptor FIRST_VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("firstVisitedPage.out.field")
                    .description("the name of the field containing the first visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("firstVisitedPage")
                    .build();

    public static final PropertyDescriptor LAST_VISITED_PAGE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("lastVisitedPage.out.field")
                    .description("the name of the field containing the last visited page => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("lastVisitedPage")
                    .build();

    public static final PropertyDescriptor IS_SESSION_ACTIVE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("isSessionActive.out.field")
                    .description("the name of the field stating whether the session is active or not => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("is_sessionActive")
                    .build();

    public static final PropertyDescriptor IS_SINGLE_PAGE_VISIT_FIELD =
            new PropertyDescriptor.Builder()
                    .name("isSinglePageVisit.out.field")
                    .description("the name of the field stating whether the session is single page visit or not => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("is_single_page_visit")
                    .build();

    public static final PropertyDescriptor SESSION_DURATION_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionDuration.out.field")
                    .description("the name of the field containing the session duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionDuration")
                    .build();

    public static final PropertyDescriptor SESSION_INACTIVITY_DURATION_FIELD =
            new PropertyDescriptor.Builder()
                    .name("sessionInactivityDuration.out.field")
                    .description("the name of the field containing the session inactivity duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("sessionInactivityDuration")
                    .build();

    public static final PropertyDescriptor EVENTS_COUNTER_FIELD =
            new PropertyDescriptor.Builder()
                    .name("eventsCounter.out.field")
                    .description("the name of the field containing the session duration => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("eventsCounter")
                    .build();

    public static final PropertyDescriptor FIRST_EVENT_DATETIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("firstEventDateTime.out.field")
                    .description("the name of the field containing the date of the first event => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("firstEventDateTime")
                    .build();

    public static final PropertyDescriptor LAST_EVENT_DATETIME_FIELD =
            new PropertyDescriptor.Builder()
                    .name("lastEventDateTime.out.field")
                    .description("the name of the field containing the date of the last event => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("lastEventDateTime")
                    .build();

    public static final PropertyDescriptor NEW_SESSION_REASON_FIELD =
            new PropertyDescriptor.Builder()
                    .name("newSessionReason.out.field")
                    .description("the name of the field containing the reason why a new session was created => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("reasonForNewSession")
                    .build();

    public static final PropertyDescriptor TRANSACTION_IDS =
            new PropertyDescriptor.Builder()
                    .name("transactionIds.out.field")
                    .description("the name of the field containing all transactionIds => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("transactionIds")
                    .build();

    /**
     * The source identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_SOURCE = "source";

    /**
     * The medium identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_MEDIUM = "medium";

    /**
     * The campaign identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN = "campaign";

    /**
     * The content identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_CONTENT = "content";

    /**
     * The term/keyword identified for the web session
     */
    public static final String SOURCE_OF_TRAFFIC_FIELD_KEYWORD = "keyword";

    protected static final String PROP_SOURCE_OF_TRAFFIC_PREFIX = "source_of_traffic.prefix";
    public static final String DEFAULT_SOURCE_OF_TRAFFIC_PREFIX = "source_of_traffic_";
    public static final String DIRECT_TRAFFIC = "direct";

    public static final PropertyDescriptor SOURCE_OF_TRAFFIC_PREFIX =
            new PropertyDescriptor.Builder()
                 .name(PROP_SOURCE_OF_TRAFFIC_PREFIX)
                 .description("Prefix for the source of the traffic related fields")
                 .required(false)
                 .defaultValue(DEFAULT_SOURCE_OF_TRAFFIC_PREFIX)
                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                 .build();

    public static final PropertyDescriptor ZONEID_CONF =
            new PropertyDescriptor.Builder()
                    .name(PROP_ES_INDEX_SUFFIX_TIMEZONE)
                    .description("The timezone to use to aprse timestamp into string date (for index names). See " +
                            PROP_ES_EVENT_INDEX_SUFFIX_FORMATTER + " and " + PROP_ES_SESSION_INDEX_SUFFIX_FORMATTER +
                            ". By default the system timezone is used. Supported by current system is : " + ZoneId.getAvailableZoneIds())
                    .required(false)
                    .addValidator(StandardValidators.ZONE_ID_VALIDATOR)
                    .build();

    public static final String defaultOutputFieldNameForEsIndex = "es_index";
    public static final PropertyDescriptor OUTPUT_FIELD_NAME_FOR_ES_INDEX =
            new PropertyDescriptor.Builder()
                    .name("record.es.index.output.field.name")
                    .description("The field name where index name to store record will be stored")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue(defaultOutputFieldNameForEsIndex)
                    .build();

    public static final String defaultOutputFieldNameForEsType = "es_type";
    public static final PropertyDescriptor OUTPUT_FIELD_NAME_FOR_ES_TYPE =
            new PropertyDescriptor.Builder()
                    .name("record.es.type.output.field.name")
                    .description("The field name where type name to store record will be stored")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue(defaultOutputFieldNameForEsType)
                    .build();

    /**
     * A singleton for valid check.
     */
    public final static SessionCheckResult DAY_OVERLAP = new InvalidSessionCheckResult("Day overlap");
    public final static SessionCheckResult SESSION_TIMEDOUT = new InvalidSessionCheckResult("Session timed-out");
    public final static SessionCheckResult SOURCE_OF_TRAFFIC = new InvalidSessionCheckResult("Source of traffic differed");

    //sessions calcul
    private long _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS;
    private Collection<String> _FIELDS_TO_RETURN;
    private Collection<SessionCheck> checkers;
    //elasticsearch indices
    private String _ES_SESSION_INDEX_PREFIX;
    private DateTimeFormatter _ES_SESSION_INDEX_SUFFIX_FORMATTER;
    private String _ES_SESSION_TYPE_NAME;
    private String _ES_EVENT_INDEX_PREFIX;
    private DateTimeFormatter _ES_EVENT_INDEX_SUFFIX_FORMATTER;
    private String _ES_EVENT_TYPE_NAME;
    //events and session model
    private Event.InternalFields eventsInternalFields;
    private WebSession.InternalFields sessionInternalFields;
    private ZoneId zoneIdToUse;
    private String outputFieldNameForEsIndex;
    private String outputFieldNameForEsType;
    /**
     * If {@code true} prints additional logs.
     */
    public boolean _DEBUG = false;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
                DEBUG_CONF,
                ES_SESSION_INDEX_PREFIX_CONF,
                ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF,
                ES_SESSION_TYPE_NAME_CONF,
                ES_EVENT_INDEX_PREFIX_CONF,
                ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF,
                ES_EVENT_TYPE_NAME_CONF,
                SESSION_ID_FIELD_CONF,
                TIMESTAMP_FIELD_CONF,
                VISITED_PAGE_FIELD,
                USER_ID_FIELD,
                FIELDS_TO_RETURN,
                FIRST_VISITED_PAGE_FIELD,
                LAST_VISITED_PAGE_FIELD,
                IS_SINGLE_PAGE_VISIT_FIELD,
                IS_SESSION_ACTIVE_FIELD,
                SESSION_DURATION_FIELD,
                SESSION_INACTIVITY_DURATION_FIELD,
                SESSION_INACTIVITY_TIMEOUT_CONF,
                EVENTS_COUNTER_FIELD,
                FIRST_EVENT_DATETIME_FIELD,
                LAST_EVENT_DATETIME_FIELD,
                NEW_SESSION_REASON_FIELD,
                TRANSACTION_IDS,
                SOURCE_OF_TRAFFIC_PREFIX,
                ZONEID_CONF,
                OUTPUT_FIELD_NAME_FOR_ES_INDEX,
                OUTPUT_FIELD_NAME_FOR_ES_TYPE
        ));
    }

    @Override
    public boolean hasControllerService()
    {
        return false;
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException
    {
        super.init(context);
        this._SESSION_INACTIVITY_TIMEOUT_IN_SECONDS = context.getPropertyValue(SESSION_INACTIVITY_TIMEOUT_CONF).asLong();
        String _SESSION_ID_FIELD = context.getPropertyValue(SESSION_ID_FIELD_CONF).asString();
        String _TIMESTAMP_FIELD = context.getPropertyValue(TIMESTAMP_FIELD_CONF).asString();
        final String _VISITED_PAGE_FIELD = context.getPropertyValue(VISITED_PAGE_FIELD).asString();

        final String fieldsToReturn = context.getPropertyValue(FIELDS_TO_RETURN).asString();
        if (fieldsToReturn != null && !fieldsToReturn.isEmpty()) {
            this._FIELDS_TO_RETURN = Arrays.asList(fieldsToReturn.split(","));
        } else {
            this._FIELDS_TO_RETURN = Collections.emptyList();
        }

        final String _USERID_FIELD = context.getPropertyValue(USER_ID_FIELD).asString();
        final String _FIRST_VISITED_PAGE_FIELD = context.getPropertyValue(FIRST_VISITED_PAGE_FIELD).asString();
        final String _LAST_VISITED_PAGE_FIELD = context.getPropertyValue(LAST_VISITED_PAGE_FIELD).asString();
        String _IS_SINGLE_PAGE_VISIT = context.getPropertyValue(IS_SINGLE_PAGE_VISIT_FIELD).asString();
        String _IS_SESSION_ACTIVE_FIELD = context.getPropertyValue(IS_SESSION_ACTIVE_FIELD).asString();
        String _SESSION_DURATION_FIELD = context.getPropertyValue(SESSION_DURATION_FIELD).asString();
        String _EVENTS_COUNTER_FIELD = context.getPropertyValue(EVENTS_COUNTER_FIELD).asString();
        final String _FIRST_EVENT_DATETIME_FIELD = context.getPropertyValue(FIRST_EVENT_DATETIME_FIELD).asString();
        final String _LAST_EVENT_DATETIME_FIELD = context.getPropertyValue(LAST_EVENT_DATETIME_FIELD).asString();
        String _SESSION_INACTIVITY_DURATION_FIELD = context.getPropertyValue(SESSION_INACTIVITY_DURATION_FIELD)
                .asString();
        final String _NEW_SESSION_REASON_FIELD = context.getPropertyValue(NEW_SESSION_REASON_FIELD).asString();
        final String _TRANSACTION_IDS = context.getPropertyValue(TRANSACTION_IDS).asString();

        final String sotPrefix = context.getPropertyValue(SOURCE_OF_TRAFFIC_PREFIX).asString();

        final String _SOT_SOURCE_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_SOURCE;
        final String _SOT_CAMPAIGN_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN;
        final String _SOT_MEDIUM_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_MEDIUM;
        final String _SOT_CONTENT_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CONTENT;
        final String _SOT_KEYWORD_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_KEYWORD;

        //Sessions indices
        this._ES_SESSION_INDEX_PREFIX = context.getPropertyValue(ES_SESSION_INDEX_PREFIX_CONF).asString();
        this._ES_SESSION_INDEX_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern(
                context.getPropertyValue(ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF).asString()
        );
        this._ES_SESSION_TYPE_NAME = context.getPropertyValue(ES_SESSION_TYPE_NAME_CONF).asString();
        //Events indices
        this._ES_EVENT_INDEX_PREFIX = context.getPropertyValue(ES_EVENT_INDEX_PREFIX_CONF).asString();
        this._ES_EVENT_INDEX_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern(
                context.getPropertyValue(ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF).asString()
        );
        this._ES_EVENT_TYPE_NAME = context.getPropertyValue(ES_EVENT_TYPE_NAME_CONF).asString();

        this.outputFieldNameForEsIndex = context.getPropertyValue(OUTPUT_FIELD_NAME_FOR_ES_INDEX).asString();
        this.outputFieldNameForEsType = context.getPropertyValue(OUTPUT_FIELD_NAME_FOR_ES_TYPE).asString();

        this.eventsInternalFields = new Event.InternalFields()
                .setSessionIdField(_SESSION_ID_FIELD)
                .setTimestampField(_TIMESTAMP_FIELD)
                .setVisitedPageField(_VISITED_PAGE_FIELD)
                .setSourceOffTrafficCampaignField(_SOT_CAMPAIGN_FIELD)
                .setSourceOffTrafficContentField(_SOT_CONTENT_FIELD)
                .setSourceOffTrafficKeyWordField(_SOT_KEYWORD_FIELD)
                .setSourceOffTrafficMediumField(_SOT_MEDIUM_FIELD)
                .setSourceOffTrafficSourceField(_SOT_SOURCE_FIELD)
                .setNewSessionReasonField(_NEW_SESSION_REASON_FIELD)
                .setUserIdField(_USERID_FIELD)
                .setOriginalSessionIdField("originalSessionId")
                .setTransactionIdField("transactionId")
                .setTransactionIdsField("transactionIds");

        this.sessionInternalFields = new WebSession.InternalFields()
                .setSessionIdField(_SESSION_ID_FIELD)
                .setTimestampField(_TIMESTAMP_FIELD)
                .setSourceOffTrafficCampaignField(_SOT_CAMPAIGN_FIELD)
                .setSourceOffTrafficContentField(_SOT_CONTENT_FIELD)
                .setSourceOffTrafficKeyWordField(_SOT_KEYWORD_FIELD)
                .setSourceOffTrafficMediumField(_SOT_MEDIUM_FIELD)
                .setSourceOffTrafficSourceField(_SOT_SOURCE_FIELD)
                .setIsSessionActiveField(_IS_SESSION_ACTIVE_FIELD)
                .setSessionDurationField(_SESSION_DURATION_FIELD)
                .setSessionInactivityDurationField(_SESSION_INACTIVITY_DURATION_FIELD)
                .setEventsCounterField(_EVENTS_COUNTER_FIELD)
                .setFirstEventDateTimeField(_FIRST_EVENT_DATETIME_FIELD)
                .setFirstEventEpochSecondsField(_FIRST_EVENT_EPOCH_SECONDS_FIELD)
                .setFirstVisitedPageField(_FIRST_VISITED_PAGE_FIELD)
                .setLastEventDateTimeField(_LAST_EVENT_DATETIME_FIELD)
                .setLastEventEpochSecondsField(_LAST_EVENT_EPOCH_SECONDS_FIELD)
                .setLastVisitedPageField(_LAST_VISITED_PAGE_FIELD)
                .setTransactionIdsField(_TRANSACTION_IDS)
                .setUserIdField(_USERID_FIELD)
                .setIsSinglePageVisit(_IS_SINGLE_PAGE_VISIT);

        this.zoneIdToUse = ZoneId.systemDefault();
        if (context.getPropertyValue(ZONEID_CONF).isSet()) {
            this.zoneIdToUse = ZoneId.of(context.getPropertyValue(ZONEID_CONF).asString());
        }
        this.checkers = Arrays.asList(
                // Day overlap
                (session, event) ->
                {
                    final ZonedDateTime firstEvent = session.getFirstEvent();
                    final ZonedDateTime lastEvent = session.getLastEvent();

                    final ZonedDateTime timestamp = event.getTimestamp();

                    boolean isValid = firstEvent.getDayOfYear() == timestamp.getDayOfYear()
                            && lastEvent.getDayOfYear() == timestamp.getDayOfYear()
                            && firstEvent.getYear() == timestamp.getYear()
                            && lastEvent.getYear() == timestamp.getYear();

                    if (_DEBUG && !isValid) {
                        debug("'Day overlap' isValid=" + isValid + " session-id=" + session.getSessionId());
                    }

                    return isValid ? ValidSessionCheckResult.getInstance() : DAY_OVERLAP;
                },

                // Timeout exceeded
                (session, event) ->
                {
                    final long durationInSeconds = Duration.between(session.getLastEvent(), event.getTimestamp())
                            .getSeconds();
                    boolean isValid = durationInSeconds <= this._SESSION_INACTIVITY_TIMEOUT_IN_SECONDS;

                    if (_DEBUG && !isValid) {
                        debug("'Timeout exceeded' isValid=" + isValid + " seconds=" + durationInSeconds +
                                " timeout=" + this._SESSION_INACTIVITY_TIMEOUT_IN_SECONDS + " session-id=" + session.getSessionId());
                    }

                    return isValid ? ValidSessionCheckResult.getInstance() : SESSION_TIMEDOUT;
                },

                // One Campaign Per Session—Each visit to your site from a different campaign—organic or paid—triggers a
                // new session, regardless of the actual time elapsed in the current session.
                (session, event) ->
                {
                    boolean isValid = Objects.equals(event.getValue(_SOT_SOURCE_FIELD), DIRECT_TRAFFIC) ||
                            Objects.deepEquals(session.getSourceOfTraffic(), event.getSourceOfTraffic());

                    if (_DEBUG && !isValid) {
                        debug("'Fields of traffic' isValid=" + isValid + " session-id=" + session.getSessionId());
                    }

                    return isValid ? ValidSessionCheckResult.getInstance() : SOURCE_OF_TRAFFIC;
                });
    }

    /**
     * Processes the incoming records and returns their result.
     *
     * @param records the records to process.
     * @return the result of the processing of the incoming records.
     * @throws ProcessException if something went wrong.
     */
    @Override
    public Collection<Record> process(final ProcessContext context,
                                      final Collection<Record> records)
        throws ProcessException
    {
        return processRecords(records);
    }

    public Collection<Record> processRecords(final Collection<Record> records) {
        if (records == null || records.isEmpty()) return new ArrayList<>();
        final Collection<Events> groupOfEvents = toWebEvents(records);
        final Collection<SimpleSessionsCalculator> calculatedSessions = computeSessions(groupOfEvents);
        return addEsIndexInfoAndConcatOutput(groupOfEvents, calculatedSessions);
    }

    public Collection<SimpleSessionsCalculator> computeSessions(Collection<Events> allEvents) {
        // Applies all events to session documents and collect results.
        return allEvents.stream()
                .map(events -> {
                    SimpleSessionsCalculator sessionCalc = new SimpleSessionsCalculator(checkers,
                            _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS,
                            sessionInternalFields,
                            eventsInternalFields,
                            _FIELDS_TO_RETURN);
                    return sessionCalc.processEvents(events);
                })
                .collect(Collectors.toList());
    }

    private List<Record> addEsIndexInfoAndConcatOutput(Collection<Events> allEvents,
                                                Collection<SimpleSessionsCalculator> calculatedSessions) {
        List<Record> outputEvents = allEvents
                .stream()
                .flatMap(events -> events.getAll().stream())
                .map(event -> {
                    Record record = event.getRecord();
                    record.setStringField(outputFieldNameForEsIndex, toEventIndexName(event.getTimestamp()));
                    record.setStringField(outputFieldNameForEsType, _ES_EVENT_TYPE_NAME);
                    return record;
                })
                .collect(Collectors.toList());

        final Collection<WebSession> flattenedSessions = calculatedSessions.stream()
                .flatMap(sessionsCalculator -> sessionsCalculator.getCalculatedSessions().stream())
                .collect(Collectors.toList());
        debug("Processing done. Outcoming records size=%d ", flattenedSessions.size());
        List<Record> outputSessions = flattenedSessions.stream()
                .map(session -> {
                    Record record = session.getRecord();
                    record.setStringField(outputFieldNameForEsIndex, toSessionIndexName(session.getFirstEvent()));
                    record.setStringField(outputFieldNameForEsType, _ES_SESSION_TYPE_NAME);
                    return record;
                })
                .collect(Collectors.toList());

        return Stream.concat(
                outputEvents.stream(),
                outputSessions.stream()
        ).collect(Collectors.toList());
    }

    /**
     * Filter out record without sessionId or timestamp
     * Returns the provided remaining records as a collection of Events instances.
     *
     * Provided records are grouped by session identifier so that each Events contains all events from a specific sessionId.
     *
     * @param records a collection of records representing web-events.
     * @return the provided records as a collection of Events instances.
     */
    private Collection<Events> toWebEvents(final Collection<Record> records) {
        // Create webEvents from input records.
        // A webEvents contains all webEvent instances of its session id.
        final Collection<Events> result =
                records.stream()
                        // Remove record without session Id or timestamp.
                        .filter(record -> isFieldAssigned(record.getField(eventsInternalFields.getSessionIdField()))
                                && isFieldAssigned(record.getField(eventsInternalFields.getTimestampField())))
                        // Create web-event from record.
                        .map(record -> new Event(record, this.eventsInternalFields))
                        // Group records per session Id.
                        .collect(Collectors.groupingBy(Event::getSessionId))
                        // Ignore keys (sessionId) and stream over list of associated events.
                        .values()
                        .stream()
                        // Wrapped grouped web-events of sessionId in WebEvents.
                        .map(Events::new)
                        .collect(Collectors.toList());

        return result;
    }

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${event-index-name}.${event-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param date the ZonedDateTime of the event to store in the index.
     * @return the name of the event index corresponding to the specified date.
     */
    public String toEventIndexName(final ZonedDateTime date) {
        return Utils.buildIndexName(_ES_EVENT_INDEX_PREFIX, _ES_EVENT_INDEX_SUFFIX_FORMATTER, date, zoneIdToUse);
    }

    /**
     * Returns the name of the event index corresponding to the specified date such as
     * ${session-index-name}${session-suffix}.
     * Eg. openanalytics-webevents.2018.01.31
     *
     * @param date the ZonedDateTime timestamp of the first event of the session.
     * @return the name of the session index corresponding to the specified timestamp.
     */
    public String toSessionIndexName(final ZonedDateTime date) {
        return Utils.buildIndexName(_ES_SESSION_INDEX_PREFIX, _ES_SESSION_INDEX_SUFFIX_FORMATTER, date, zoneIdToUse);
    }

    /**
     * Facility to log debug.
     *
     * @param format the format of the String.
     * @param args the arguments.
     */
    private void debug(final String format, final Object... args)
    {
        if ( _DEBUG )
        {
            if ( args.length == 0 )
            {
                getLogger().debug(format);
            }
            else
            {
                getLogger().debug(String.format(format + "\n", args));
            }
        }
    }

}

