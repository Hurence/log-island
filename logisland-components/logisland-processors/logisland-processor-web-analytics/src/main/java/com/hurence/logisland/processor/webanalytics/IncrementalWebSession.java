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
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.processor.webanalytics.modele.*;
import com.hurence.logisland.processor.webanalytics.util.SessionsCalculator;
import com.hurence.logisland.processor.webanalytics.util.Utils;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.datastore.model.*;
import com.hurence.logisland.service.datastore.model.bool.*;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.validator.StandardValidators;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.logisland.processor.webanalytics.util.Utils.isFieldAssigned;

@Category(ComponentCategory.ANALYTICS)
@Tags({"analytics", "web", "session"})
@CapabilityDescription(
value = "This processor creates and updates web-sessions based on incoming web-events." +
        " Note that both web-sessions and web-events are stored in elasticsearch.\n" +

        " Firstly, web-events are grouped by their session identifier and processed in chronological order.\n" +
        " Then each web-session associated to each group is retrieved from elasticsearch.\n" +
        " In case none exists yet then a new web session is created based on the first web event.\n" +
        " The following fields of the newly created web session are set based on the associated web event:" +
        " session identifier, first timestamp, first visited page." +

        " Secondly, once created, or retrieved, the web session is updated by the remaining web-events.\n" +
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
        " identifier- are save in elasticsearch. And web sessions are passed to the next processor.\n" +
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
@ExtraDetailFile("./details/IncrementalWebSession-Detail.rst")
public class IncrementalWebSession
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

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE_CONF =
            new PropertyDescriptor.Builder()
                    .name("elasticsearch.client.service")
                    .description("The instance of the Controller Service to use for accessing Elasticsearch.")
                    .required(true)
                    .identifiesControllerService(ElasticsearchClientService.class)
                    .build();

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

    public static final PropertyDescriptor IS_SINGLE_PAGE_VISIT_FIELD =
            new PropertyDescriptor.Builder()
                    .name("isSinglePageVisit.out.field")
                    .description("the name of the field stating whether the session is single page visit or not => will override default value if set")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("is_single_page_visit")
                    .build();
        
    public static final PropertyDescriptor IS_SESSION_ACTIVE_FIELD =
            new PropertyDescriptor.Builder()
                    .name("isSessionActive.out.field")
                    .description("the name of the field stating whether the session is active or not => will override default value if set")
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .defaultValue("is_sessionActive")
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

    protected static final String PROP_CACHE_SERVICE = "cache.service";

    public static final PropertyDescriptor CONFIG_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name(PROP_CACHE_SERVICE)
            .description("The name of the cache service to use.")
            .required(true)
            .identifiesControllerService(CacheService.class)
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

    public static final PropertyDescriptor SOURCE_OF_TRAFFIC_PREFIX_FIELD =
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

    public static final PropertyDescriptor NUMBER_OF_FUTURE_SESSION =
            new PropertyDescriptor.Builder()
                    .name("number.of.future.session.when.event.from.past")
                    .description("The number of session it will look for when searching session of last events")
                    .required(false)
                    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                    .defaultValue("1")
                    .build();

    /**
     * A singleton for valid check.
     */
    public final static SessionCheckResult DAY_OVERLAP = new InvalidSessionCheckResult("Day overlap");
    public final static SessionCheckResult SESSION_TIMEDOUT = new InvalidSessionCheckResult("Session timed-out");
    public final static SessionCheckResult SOURCE_OF_TRAFFIC = new InvalidSessionCheckResult("Source of traffic differed");


    //services
    private ElasticsearchClientService elasticsearchClientService;
    private CacheService<String/*sessionId*/, WebSession> cacheService;
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
    //Tuning
    private final long maxNumberOfEventForCurrentSessionRequested = 10000L;
    //events and session model
    private Event.InternalFields eventsInternalFields;
    private WebSession.InternalFields sessionInternalFields;
    private ZoneId zoneIdToUse;
    private String outputFieldNameForEsIndex;
    private String outputFieldNameForEsType;
    private int numberOfFuturSessionToFetchWhenReceivingPastEvents;
    /**
     * If {@code true} prints additional logs.
     */
    public boolean _DEBUG = false;
    private long rewindCounter = 0L;

    public long getNumberOfRewindForProcInstance() {
        return rewindCounter;
    }

    public void resetNumberOfRewindForProcInstance() {
        rewindCounter = 0L;
    }

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
                SOURCE_OF_TRAFFIC_PREFIX_FIELD,
                // Service
                ELASTICSEARCH_CLIENT_SERVICE_CONF,
                CONFIG_CACHE_SERVICE,
                ZONEID_CONF,
                OUTPUT_FIELD_NAME_FOR_ES_INDEX,
                OUTPUT_FIELD_NAME_FOR_ES_TYPE,
                NUMBER_OF_FUTURE_SESSION
        ));
    }

    @Override
    public boolean hasControllerService()
    {
        return true;
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException
    {
        super.init(context);
        this.elasticsearchClientService = PluginProxy.rewrap(context.getPropertyValue(ELASTICSEARCH_CLIENT_SERVICE_CONF)
                .asControllerService());
        if (elasticsearchClientService == null)
        {
            getLogger().error("Elasticsearch client service is not initialized!");
        }
        cacheService = PluginProxy.rewrap(context.getPropertyValue(CONFIG_CACHE_SERVICE).asControllerService());
        if (cacheService == null) {
            getLogger().error("Cache service is not initialized!");
        }
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

        final String sotPrefix = context.getPropertyValue(SOURCE_OF_TRAFFIC_PREFIX_FIELD).asString();

        final String _SOT_SOURCE_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_SOURCE;
        final String _SOT_CAMPAIGN_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CAMPAIGN;
        final String _SOT_MEDIUM_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_MEDIUM;
        final String _SOT_CONTENT_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_CONTENT;
        final String _SOT_KEYWORD_FIELD = sotPrefix + SOURCE_OF_TRAFFIC_FIELD_KEYWORD;

        //Sessions indices
        this._ES_SESSION_INDEX_PREFIX = context.getPropertyValue(ES_SESSION_INDEX_PREFIX_CONF).asString();
        Objects.requireNonNull(this._ES_SESSION_INDEX_PREFIX, "Property required: " + ES_SESSION_INDEX_PREFIX_CONF);
        this._ES_SESSION_INDEX_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern(
                context.getPropertyValue(ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF).asString()
        );
        Objects.requireNonNull(this._ES_SESSION_INDEX_SUFFIX_FORMATTER, "Property required: " + ES_SESSION_INDEX_SUFFIX_FORMATTER_CONF);
        this._ES_SESSION_TYPE_NAME = context.getPropertyValue(ES_SESSION_TYPE_NAME_CONF).asString();
        Objects.requireNonNull(this._ES_SESSION_TYPE_NAME, "Property required: " + ES_SESSION_TYPE_NAME_CONF);
        //Events indices
        this._ES_EVENT_INDEX_PREFIX = context.getPropertyValue(ES_EVENT_INDEX_PREFIX_CONF).asString();
        Objects.requireNonNull(this._ES_EVENT_INDEX_PREFIX, "Property required: " + ES_EVENT_INDEX_PREFIX_CONF);
        this._ES_EVENT_INDEX_SUFFIX_FORMATTER = DateTimeFormatter.ofPattern(
                context.getPropertyValue(ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF).asString()
        );
        Objects.requireNonNull(this._ES_EVENT_INDEX_SUFFIX_FORMATTER, "Property required: " + ES_EVENT_INDEX_SUFFIX_FORMATTER_CONF);
        this._ES_EVENT_TYPE_NAME = context.getPropertyValue(ES_EVENT_TYPE_NAME_CONF).asString();
        Objects.requireNonNull(this._ES_EVENT_TYPE_NAME, "Property required: " + ES_EVENT_TYPE_NAME_CONF);

        this.outputFieldNameForEsIndex = context.getPropertyValue(OUTPUT_FIELD_NAME_FOR_ES_INDEX).asString();
        this.outputFieldNameForEsType = context.getPropertyValue(OUTPUT_FIELD_NAME_FOR_ES_TYPE).asString();

        this.numberOfFuturSessionToFetchWhenReceivingPastEvents = context.getPropertyValue(NUMBER_OF_FUTURE_SESSION).asInteger();

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
        final Collection<String> inputDivolteSessions = groupOfEvents.stream()
                .map(Events::getOriginalSessionId)
                .collect(Collectors.toList());
        final Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping = getMapping(inputDivolteSessions);

        //This method may update lastSessionMapping to current session (only the name will be used) when rewind is detected !
        SplittedEvents splittedEvents = handleRewindAndGetAllNeededEvents(groupOfEvents, lastSessionMapping);

        final Collection<Events> allEvents = splittedEvents.getAllEvents().collect(Collectors.toList());
        final Collection<SessionsCalculator> calculatedSessions = computeSessions(lastSessionMapping, splittedEvents, allEvents);

        updateCacheWithLastSessions(calculatedSessions);
        return addEsIndexInfoAndConcatOutput(allEvents, calculatedSessions);
    }

    public Collection<SessionsCalculator> computeSessions(Map<String, Optional<WebSession>> lastSessionMapping, SplittedEvents splittedEvents, Collection<Events> allEvents) {
        final Collection<SessionsCalculator> calculatedSessions;
        if (splittedEvents.isThereEventsFromPast()) {
            Set<String> sessionsInRewind = splittedEvents.getDivolteSessionsWithEventsFromPast();
            calculatedSessions = this.processEvents(allEvents, lastSessionMapping, sessionsInRewind);
        } else {
            calculatedSessions = this.processEvents(allEvents, lastSessionMapping);
        }
        return calculatedSessions;
    }

    public void updateCacheWithLastSessions(Collection<SessionsCalculator> calculatedSessions) {
        calculatedSessions
                .forEach(sessionsCalculator -> {
                    String divolteSession = sessionsCalculator.getDivolteSessionId();
                    WebSession lastSession = sessionsCalculator.getCalculatedSessions().stream()
                            .filter(session -> session.getSessionId().equals(sessionsCalculator.getLastSessionId()))
                            .findFirst().get();
                    cacheService.set(divolteSession, lastSession);
                });
    }

    private List<Record> addEsIndexInfoAndConcatOutput(Collection<Events> allEvents,
                                                Collection<SessionsCalculator> calculatedSessions) {
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

    private SplittedEvents handleRewindAndGetAllNeededEvents(final Collection<Events> groupOfEvents,
                                                             final Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping) {

        final SplittedEvents splittedEvents = getSplittedEvents(groupOfEvents, lastSessionMapping);
        final Collection<Events> eventsFromPast = splittedEvents.getEventsfromPast();
        if (eventsFromPast.isEmpty()) {
            return splittedEvents;
        }
        rewindCounter++;//may wish to only do this on a MockProcessor extending this processor
//        deleteFuturSessions(eventsFromPast);//TODO remove deletion of future sessions ?
        Collection<Event> eventsFromEs = getNeededEventsFromEs(splittedEvents, lastSessionMapping);
        Map<String/*divolteSessionId*/, List<Event>> eventsFromEsByDivolteSessionId = eventsFromEs
                .stream()
                .collect(Collectors.groupingBy(Event::getOriginalSessionIdOrSessionId));
        //merge those events into the lists
        for (Events events : eventsFromPast) {
            List<Event> eventsFromEsForSession = eventsFromEsByDivolteSessionId.getOrDefault(events.getOriginalSessionId(), Collections.emptyList());
            events.addAll(eventsFromEsForSession);//les events deja presents sont prioritaire. Si meme id.
        }
        return splittedEvents;
    }

    /**
     * For all Events, find current session of the first event (from ES).
     * Then Find all events from this session that are before or at the same time than the first event in input.
     * Return those events.
     * @param splittedEvents
     * @return
     */
    private Collection<Event> getNeededEventsFromEs(final SplittedEvents splittedEvents,
                                                    final Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping) {

        /*
            Pour chaque events trouver les evènements de la session en cour nécessaire.
            C'est à dire requêter tous les events de la sessionId et timestamp <= firstEventTs(input events)
            Il faut aussi récupérer tous les events de la sessionId et timestamp >= lastEventTs(input events) Attention tout les events pas que du passé
        */
        //TODO in order to this to work we need to get all events in es from Tmin to Tmax plus eventSessionTimin eventSessionTmax
        //la on merge seulement dans events from past
        //get all events but only for events containing elements from the past
        final Collection<Events> allEvents = splittedEvents.getAllEventsThatContainsEventsFromPast().collect(Collectors.toList());
        final Set<String> divoltSessionIds = allEvents.stream().map(Events::getOriginalSessionId).collect(Collectors.toSet());
        try {//TODO find another solution
            Thread.sleep(600L);//Unfortunately without this some test randomly fails. It seems that session indices are not properly refreshed..
        } catch (InterruptedException e) {
            getLogger().error("error while waiting 200ms", e);
        }
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(_ES_SESSION_INDEX_PREFIX + "*", 100000L);
        final Map<String/*divolteSession*/, Optional<WebSession>> sessionsOfFirstEvents = requestCurrentSessionsToEs(allEvents);
        divoltSessionIds.forEach(divoltSessionId -> {
            if (!sessionsOfFirstEvents.containsKey(divoltSessionId)) {
                //Sinon l'event est avant tout les autres events dans ES donc on recommence à la première session...
                sessionsOfFirstEvents.put(divoltSessionId, Optional.empty());
            }
        });
        final Map<String/*divolteSession*/, Set<WebSession>> sessionsOfLastEvents = requestSessionsOfLastEventToEs(allEvents);
        divoltSessionIds.forEach(divoltSessionId -> {
            if (!sessionsOfLastEvents.containsKey(divoltSessionId)) {
                //Sinon l'event est après tout les autres events dans ES donc on va de toute manière tous les récupérer...
                sessionsOfLastEvents.put(divoltSessionId, Collections.emptySet());
            }
        });
        uptadateLastSessionsMapping(lastSessionMapping, sessionsOfFirstEvents);
        resetCacheWithSessions(sessionsOfFirstEvents);//TODO is this necessary ? as cache is updated at end of process and used only at start of process...
        Collection<EventsToQueryInfo> eventsToQueryInfo = buildEventsRange(allEvents,
                sessionsOfFirstEvents,
                sessionsOfLastEvents);
        Collection<Event> neededEventsFromEs = requestEventsFromSessionsToEs(eventsToQueryInfo);
        return neededEventsFromEs;
    }

    private Collection<EventsToQueryInfo> buildEventsRange(Collection<Events> events,
                                                           final Map<String/*divolteSession*/, Optional<WebSession>> sessionsOfFirstEvents,
                                                           final Map<String/*divolteSession*/,  Set<WebSession>> sessionsOfLastEvents) {
        return events.stream()
                .map(eventsForDivoltSession ->  {
                    return buildEventsRangefromEvents(eventsForDivoltSession,
                            sessionsOfFirstEvents,
                            sessionsOfLastEvents);
                })
                .collect(Collectors.toList());
    }

    public EventsToQueryInfo buildEventsRangefromEvents(Events events,
                                                        Map<String/*divolteSession*/, Optional<WebSession>> sessionsOfFirstEvents,
                                                        Map<String/*divolteSession*/, Set<WebSession>> sessionsOfLastEvents) {
        final long min = events.first().getEpochTimeStampMilli();
        final long max = events.last().getEpochTimeStampMilli();
        final String divoltSession = events.getOriginalSessionId();//This is divolteSessionId car viens de l'input et pas d'ES
        final Optional<String> firstSession = sessionsOfFirstEvents.get(divoltSession)
                .map(WebSession::getSessionId);
        final Set<String> lastSessions = sessionsOfLastEvents.get(divoltSession).stream()
                .map(WebSession::getSessionId)
                .collect(Collectors.toSet());
        List<String> indicesToQuery = events.getAll().stream()
                .map(event -> {
                    return toEventIndexName(event.getTimestamp());
                })
                .distinct()
                .collect(Collectors.toList());
        return new EventsToQueryInfo(divoltSession, firstSession, lastSessions, min, max, indicesToQuery);
    }

    private static class EventsToQueryInfo {

        public final List<String> indicesToQuery;
        public final String divoltSession;
        public final Optional<String> currentSession;
        public final Set<String> lastSessions;
        public final long min;
        public final long max;

        public EventsToQueryInfo(String divoltSession,
                                 Optional<String> currentSession,
                                 Set<String> lastSessions,
                                 long min,
                                 long max,
                                 List<String> indicesToQuery) {
            this.divoltSession = divoltSession;
            this.currentSession = currentSession;
            this.lastSessions = lastSessions;
            this.min = min;
            this.max = max;
            this.indicesToQuery = indicesToQuery;
        }


    }

    /**
     * update lastSessionMapping with currentSessionsOfEvents values
     * @param lastSessionMapping
     * @param currentSessionsOfEvents
     */
    private void uptadateLastSessionsMapping(Map<String, Optional<WebSession>> lastSessionMapping,
                                             Map<String, Optional<WebSession>> currentSessionsOfEvents) {
        currentSessionsOfEvents.forEach(lastSessionMapping::put);
    }

    private void resetCacheWithSessions(Map<String, Optional<WebSession>> currentSessionsOfEvents) {
        currentSessionsOfEvents.forEach((divolteSessions, session) -> this.cacheService.set(divolteSessions, session.orElse(null)));
    }


//    private Collection<Event> requestEventsFromSessionsToEs(Collection<WebSession> currentSessionsOfEvents,
//                                                            Map<String, ZonedDateTime> divoltSessionToFirstEvent) {
//        MultiQueryResponseRecord eventsRsp = getMissingEventsForSessionsFromEs(currentSessionsOfEvents, divoltSessionToFirstEvent);
//        return convertEsRToEvents(eventsRsp);
//    }

    private Collection<Event> requestEventsFromSessionsToEs(Collection<EventsToQueryInfo> eventsToQuery) {
        MultiQueryResponseRecord eventsRsp = getMissingEventsForSessionsFromEs(eventsToQuery);
        return convertEsRToEvents(eventsRsp);
    }


    private Collection<Event> convertEsRToEvents(MultiQueryResponseRecord eventsRsp) {
        final List<Event> events = new ArrayList<>();
        eventsRsp.getResponses().forEach(rsp -> {
            if (rsp.getTotalMatched() > maxNumberOfEventForCurrentSessionRequested) {
                Event firstEvent = mapToEvent(rsp.getDocs().get(0).getRetrievedFields());
                String errorMsg = "A query to search events for current session exceeds " + maxNumberOfEventForCurrentSessionRequested +
                        " events ! either increases maximum expected either verify if this sessions '" +
                        firstEvent.getSessionId() +"' has really this much of events !";
                getLogger().error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            rsp.getDocs().stream().forEach(rspRecord -> {
                events.add(mapToEvent(rspRecord.getRetrievedFields()));
            });
        });
        return events;
    }

/*
    GET webenvent_index_prefix<wildcard>/_search
    {
        "query": {
           "bool": {
            "should": [
              {
                 "term": {
                      "sessionId.raw": {
                        "value": "<session_id_first_event>"
                      }
                 }
              },
              {
                "bool": {
                    "must": [
                        {
                            "wildcard": {
                              "sessionId.raw": {
                                "value": "<divolte_session_id>*"
                              }
                            }
                        },{
                            "range": {
                              "h2kTimestamp": {
                                "lte": Tmax,
                                "gte": Tmin
                              }
                            }
                        }
                     ]
                }
              },
               {
                "bool": {
                    "should": [
                        {
                             "term": {
                                  "sessionId.raw": {
                                    "value": "<session_id_last_event>"
                                  }
                             }
                        },
                        ...
                        {
                             "term": {
                                  "sessionId.raw": {
                                    "value": "<session_id_last_event + (N - 1)>"
                                  }
                             }
                        }
                     ]
                }
              }
            ]
           }
        },
        "size": 10000
    }
*/
    private MultiQueryResponseRecord getMissingEventsForSessionsFromEs(Collection<EventsToQueryInfo> eventsToQuery) {
        final List<QueryRecord> queries = new ArrayList<>();
        eventsToQuery.forEach(info -> {
            BoolQueryRecordRoot boolQueryForAllEventsFromMinToMax = new BoolQueryRecordRoot()
                    .addBoolQuery(
                            new WildCardQueryRecord(eventsInternalFields.getSessionIdField() + ".raw", info.divoltSession + "*"),
                            BoolCondition.MUST
                    ).addBoolQuery(
                            new RangeQueryRecord(eventsInternalFields.getTimestampField())
                                    .setFrom(info.min)
                                    .setTo(info.max)
                                    .setIncludeUpper(true)
                                    .setIncludeLower(true),
                            BoolCondition.MUST
                    );
            QueryRecord query = new QueryRecord()
                    .addCollections(info.indicesToQuery)
                    .addType(_ES_EVENT_TYPE_NAME)
                    .addBoolQuery(boolQueryForAllEventsFromMinToMax, BoolCondition.SHOULD)//or this
                    .size(10000);
            info.currentSession.ifPresent(session -> {
                BoolQueryRecordRoot boolQueryForEventsOfFirstSession = new BoolQueryRecordRoot()
                        .addBoolQuery(
                                new TermQueryRecord(eventsInternalFields.getSessionIdField() + ".raw", session),
                                BoolCondition.MUST
                        );
                query.addBoolQuery(boolQueryForEventsOfFirstSession, BoolCondition.SHOULD);
            });
            if (!info.lastSessions.isEmpty()) {
                final BoolQueryRecordRoot boolQueryForEventsOfLastSession = new BoolQueryRecordRoot();
                info.lastSessions.forEach(session -> {
                    boolQueryForEventsOfLastSession.addBoolQuery(
                            new TermQueryRecord(eventsInternalFields.getSessionIdField() + ".raw", session),
                            BoolCondition.SHOULD
                    );
                });
                query.addBoolQuery(boolQueryForEventsOfLastSession, BoolCondition.SHOULD);
            }
            queries.add(query);
        });
        MultiQueryRecord multiQuery = new MultiQueryRecord(queries);
        String[] indicesToWaitFor = eventsToQuery.stream()
                .flatMap(info -> {
                    return  info.indicesToQuery.stream();
                })
                .toArray(String[]::new);
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(indicesToWaitFor, 100000L);
        return elasticsearchClientService.multiQueryGet(multiQuery);
    }

    private Map<String, Optional<WebSession>> requestCurrentSessionsToEs(Collection<Events> events) {
        MultiQueryResponseRecord sessionEsRsp = requestSessionsOfFirstEvent(events);
        return checkAndTransformToCurrentSessionMap(sessionEsRsp);
    }

    private Map<String, Set<WebSession>> requestSessionsOfLastEventToEs(Collection<Events> events) {
        MultiQueryResponseRecord sessionEsRsp = requestSessionsOfLastEvent(events);
        return checkAndTransformToLastSessionMap(sessionEsRsp);
    }
    /**
     * ensure there is only one response by query and return as map with divolte session as key.
     * @param sessionEsRsp
     * @return
     */
    private Map<String, Optional<WebSession>> checkAndTransformToCurrentSessionMap(MultiQueryResponseRecord sessionEsRsp) {
        final Map<String, Optional<WebSession>> sessionMap = new HashMap<>();
        for (QueryResponseRecord rsp : sessionEsRsp.getResponses()) {
            if (rsp.getTotalMatched() >= 1) {
                WebSession currentSession = mapToSession(rsp.getDocs().get(0).getRetrievedFields());
                sessionMap.put(currentSession.getOriginalSessionId(), Optional.of(currentSession));
            }
        }
        return sessionMap;
    }

    private Map<String, Set<WebSession>> checkAndTransformToLastSessionMap(MultiQueryResponseRecord sessionEsRsp) {
        final Map<String, Set<WebSession>> sessionMap = new HashMap<>();
        for (QueryResponseRecord rsp : sessionEsRsp.getResponses()) {
            if (rsp.getTotalMatched() >= 1) {
                Set<WebSession> sessions = rsp.getDocs().stream()
                        .map(ResponseRecord::getRetrievedFields)
                        .map(this::mapToSession)
                        .collect(Collectors.toSet());
                sessionMap.put(sessions.stream().findFirst().get().getOriginalSessionId(), sessions);
            }
        }
        return sessionMap;
    }

    //    GET new_openanalytics_websessions-*/_search
//{
//    "query": {
//      "bool": {
//          "must": [
//              {
//                  "wildcard": {
//                      "sessionId.raw": {
//                          "value": "<divolte_session_id>*"
//                      }
//                  }
//              },
//              {
//                  "range": {
//                       "firstEventEpochSeconds": {
//                          "lte": TmaxEvent
//                       }
//                  }
//              }
//        ]
//      },
//    "sort": [
//        {
//            "firstEventEpochSeconds": {
//               "order": "desc"
//            }
//        }
//    ],
//    "size": 1
//}

    /**
     * find current session OR closer >
     * @param events
     * @return
     */
    private MultiQueryResponseRecord requestSessionsOfFirstEvent(Collection<Events> events) {
        final List<QueryRecord> queries = new ArrayList<>();
        events.stream().forEach(eventsForDivoltSession -> {
            String divolteSession = eventsForDivoltSession.getOriginalSessionId();
            long epochSecondLastEvent = eventsForDivoltSession.first().getEpochTimeStampSeconds();
            QueryRecord query = new QueryRecord()
                    .addCollection(_ES_SESSION_INDEX_PREFIX + "*")
                    .addType(_ES_SESSION_TYPE_NAME)
                    .addBoolQuery(
                            new WildCardQueryRecord(sessionInternalFields.getSessionIdField() + ".raw", divolteSession + "*"),
                            BoolCondition.MUST
                    )
                    .addBoolQuery(
                            new RangeQueryRecord(_FIRST_EVENT_EPOCH_SECONDS_FIELD)
                                    .setTo(epochSecondLastEvent)
                                    .setIncludeUpper(true),
                            BoolCondition.MUST
                    )
                    .addSortQuery(
                            new SortQueryRecord(_FIRST_EVENT_EPOCH_SECONDS_FIELD, SortOrder.DESC)
                    )
                    .size(1);
            queries.add(query);
        });
        MultiQueryRecord multiQuery = new MultiQueryRecord(queries);
        MultiQueryResponseRecord rsp = elasticsearchClientService.multiQueryGet(multiQuery);
        return rsp;
    }

//GET new_openanalytics_websessions-*/_search
//{
//    "query": {
//      "bool": {
//          "must": [
//              {
//                  "wildcard": {
//                      "sessionId.raw": {
//                          "value": "<divolte_session_id>*"
//                      }
//                  }
//              },
//              {
//                  "range": {
//                       "lastEventEpochSeconds": {
//                          "gte": TmaxEvent
//                       }
//                  }
//              }
//        ]
//      },
//    "sort": [
//        {
//            "lastEventEpochSeconds": {
//               "order": "asc"
//            }
//        }
//    ],
//    "size": numberOfFuturSessionToFetchWhenReceivingPastEvents
//}
    /**
     * request current session or closer >
     * @param events
     * @return
     */
    private MultiQueryResponseRecord requestSessionsOfLastEvent(Collection<Events> events) {
        final List<QueryRecord> queries = new ArrayList<>();
        events.stream().forEach(eventsForDivoltSession -> {
            String divolteSession = eventsForDivoltSession.getOriginalSessionId();
            long epochSecondLastEvent = eventsForDivoltSession.last().getEpochTimeStampSeconds();
            QueryRecord query = new QueryRecord()
                    .addCollection(_ES_SESSION_INDEX_PREFIX + "*")
                    .addType(_ES_SESSION_TYPE_NAME)
                    .addBoolQuery(
                            new WildCardQueryRecord(sessionInternalFields.getSessionIdField() + ".raw", divolteSession + "*"),
                            BoolCondition.MUST
                    )
                    .addBoolQuery(
                            new RangeQueryRecord(_LAST_EVENT_EPOCH_SECONDS_FIELD)
                                    .setFrom(epochSecondLastEvent)
                                    .setIncludeLower(true),
                            BoolCondition.MUST
                    )
                    .addSortQuery(
                            new SortQueryRecord(_LAST_EVENT_EPOCH_SECONDS_FIELD, SortOrder.ASC)
                    )
                    .size(numberOfFuturSessionToFetchWhenReceivingPastEvents);
            queries.add(query);
        });
        MultiQueryRecord multiQuery = new MultiQueryRecord(queries);
        MultiQueryResponseRecord rsp = elasticsearchClientService.multiQueryGet(multiQuery);
        return rsp;
    }

    private SplittedEvents getSplittedEvents(Collection<Events> groupOfEvents,
                                             Map<String/*sessionId*/, Optional<WebSession>> lastSessionMapping) {
        final Collection<Events> eventsFromPast = new ArrayList<>();
        final Collection<Events> eventsOk = new ArrayList<>();
        for (Events events : groupOfEvents) {
            Optional<WebSession> lastSession = lastSessionMapping.get(events.getOriginalSessionId());
            if (lastSession.isPresent() &&
                    lastSession.get().timestampFromPast(events.first().getTimestamp())) {
                eventsFromPast.add(events);
            } else {
                eventsOk.add(events);
            }
        }
        return new SplittedEvents(eventsOk, eventsFromPast);
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
     * Processes the provided events and returns their resulting sessions.
     * All sessions are retrieved from the cache or elasticsearch and then updated with the specified web events.
     * One serie of events may result into multiple sessions.
     *
     * @param webEvents the web events from a same session to process.
     * @return web sessions resulting of the processing of the web events.
     */
    private Collection<SessionsCalculator> processEvents(final Collection<Events> webEvents,
                                                         final Map<String, Optional<WebSession>> lastSessionMapping,
                                                         final Set<String> sessionsInRewind) {
        // Applies all events to session documents and collect results.
        return webEvents.stream()
                .map(events -> {
                    String divolteSession = events.getOriginalSessionId();
                    SessionsCalculator sessionCalc = new SessionsCalculator(checkers,
                            _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS,
                            sessionInternalFields,
                            eventsInternalFields,
                            _FIELDS_TO_RETURN,
                            divolteSession);
                    if (lastSessionMapping.get(divolteSession).isPresent()) {
                        boolean isRewind = sessionsInRewind.contains(divolteSession);
                        if (isRewind) {//only keep sessionId but not counters etc because we will recompute the whole session
                            return sessionCalc.processEvents(events, lastSessionMapping.get(divolteSession).get().getSessionId());
                        } else {
                            return sessionCalc.processEventsKnowingLastSession(events, lastSessionMapping.get(divolteSession).get());
                        }
                    } else {
                        return sessionCalc.processEventsKnowingLastSession(events, null);
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * Processes the provided events and returns their resulting sessions.
     * All sessions are retrieved from the cache or elasticsearch and then updated with the specified web events.
     * One serie of events may result into multiple sessions.
     *
     * @param webEvents the web events from a same session to process.
     * @return web sessions resulting of the processing of the web events.
     */
    private Collection<SessionsCalculator> processEvents(final Collection<Events> webEvents,
                                                         final Map<String, Optional<WebSession>> lastSessionMapping) {
        // Applies all events to session documents and collect results.
        return webEvents.stream()
                .map(events -> {
                    String divolteSession = events.getOriginalSessionId();
                    SessionsCalculator sessionsCalculator = new SessionsCalculator(checkers,
                            _SESSION_INACTIVITY_TIMEOUT_IN_SECONDS,
                            sessionInternalFields,
                            eventsInternalFields,
                            _FIELDS_TO_RETURN,
                            divolteSession);
//                    if (lastSessionMapping.get(divolteSession).isPresent()) {
                        return sessionsCalculator.processEventsKnowingLastSession(events, lastSessionMapping.get(events.getOriginalSessionId()).orElse(null));
//                    } else {
//                        return sessionsCalculator.processEventsKnowingLastSession(events, null);
//                    }
                })
                .collect(Collectors.toList());
    }

//    GET new_openanalytics_websessions-*/_search
//{
//    "query": {
//    "wildcard": {
//        "sessionId.raw": {
//            "value": "<divolte_session>*"
//        }
//    }
//},
//    "sort": [
//        {
//            "h2kTimestamp": {
//            "order": "desc"
//        }
//    }
//  ],
//    "size": 1
//}
    /**
     * query all last hurence session for each divolte session in ES if it is not already present in cache.
     * THen fill up cache with new data, finally return the <code>mapping[divolte_session,last_hurence_session]</code>
     *
     * if a last session is found it is filled with [divolte_session,last_hurence_session]
     * If there is no session found it is filled with [divolte_session,Optional.empty]
     * @param divolteSessions
     * @return
     */
    private Map<String/*divolteSession*/, Optional<WebSession>/*lastHurenceSession*/> getMapping(final Collection<String> divolteSessions) {
        final Map<String, Optional<WebSession>> mappingToReturn = new HashMap<>();
        final List<QueryRecord> sessionsRequests = new ArrayList<>();
        divolteSessions.forEach(divoltSession -> {
            WebSession cachedSession = cacheService.get(divoltSession);
            if (cachedSession != null) {
                mappingToReturn.put(divoltSession, Optional.of(cachedSession));
            } else {
                QueryRecord request = new QueryRecord()
                        .addCollection(_ES_SESSION_INDEX_PREFIX + "*")
                        .addType(_ES_SESSION_TYPE_NAME)
                        .addBoolQuery(
                                new WildCardQueryRecord(sessionInternalFields.getSessionIdField() + ".raw", divoltSession + "*"),
                                BoolCondition.MUST
                        )
                        .addSortQuery(new SortQueryRecord(sessionInternalFields.getTimestampField(), SortOrder.DESC))
                        .size(1);//only need the last mapping
                sessionsRequests.add(request);
            }
        });
        if (sessionsRequests.isEmpty()) return mappingToReturn;
        this.elasticsearchClientService.waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(_ES_SESSION_INDEX_PREFIX + "*", 100000L);
        MultiQueryResponseRecord multiQueryResponses = this.elasticsearchClientService.multiQueryGet(
                new MultiQueryRecord(sessionsRequests)
        );
        Map<String, WebSession> sessionsFromEs = transformIntoWebSessions(multiQueryResponses);

        divolteSessions.forEach(divoltSession -> {
            if (!mappingToReturn.containsKey(divoltSession)) {
                if (!sessionsFromEs.containsKey(divoltSession)) {
                    mappingToReturn.put(divoltSession, Optional.empty());
                } else {
                    WebSession lastSessionInEs = sessionsFromEs.get(divoltSession);
                    cacheService.set(divoltSession, lastSessionInEs);
                    mappingToReturn.put(divoltSession, Optional.of(lastSessionInEs));
                }
            }
        });
        return mappingToReturn;
    }

    private Map<String/*divolteId*/, WebSession> transformIntoWebSessions(MultiQueryResponseRecord multiQueryResponses) {
        return multiQueryResponses.getDocs().stream()
                .map(doc -> {
                    return mapToSession(doc.getRetrievedFields());
                })
                .collect(Collectors.toMap(
                        WebSession::getOriginalSessionId,
                        Function.identity()
                ));
    }

    /**
     * Returns a new record based on the specified map that represents a web session in elasticsearch.
     *
     * @param sourceAsMap the web session stored in elasticsearch.
     * @return a new record based on the specified map that represents a web session in elasticsearch.
     */
    private WebSession mapToSession(final Map<String, Object> sourceAsMap) {
        return WebSession.fromMap(sourceAsMap, this.sessionInternalFields, OUTPUT_RECORD_TYPE);
    }

    /**
     * return a new Event based on the specified map that represents a web event in elasticsearch.
     *
     * @param sourceAsMap the event stored in elasticsearch.
     * @return a new Event based on the specified map that represents a web event in elasticsearch.
     */
    private Event mapToEvent(final Map<String, Object> sourceAsMap) {
        return Event.fromMap(sourceAsMap, this.eventsInternalFields, OUTPUT_RECORD_TYPE);
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

