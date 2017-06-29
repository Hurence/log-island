package com.hurence.logisland.service.rocksdb;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.service.rocksdb.delete.DeleteRequest;
import com.hurence.logisland.service.rocksdb.delete.DeleteResponse;
import com.hurence.logisland.service.rocksdb.get.GetRequest;
import com.hurence.logisland.service.rocksdb.get.GetResponse;
import com.hurence.logisland.service.rocksdb.put.ValuePutRequest;
import com.hurence.logisland.service.rocksdb.scan.RocksIteratorHandler;
import com.hurence.logisland.validator.StandardValidators;
import org.rocksdb.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of rocksDb 5.4.0
 *
 * To use a rockdb you need to specify
 * _a path to the db
 * _The options of the db (optionnal)
 * _if readAndWrite mode The description of each column family currently present in the db otherwise you get a "InvalidArgument" exception
 * _if readOnly you can specify a subset of all the columns family
 *
 * You can specify configuration of rocksDb with a File, using service properties or even dynamic properties
 * if some properties are not yet defined in the service
 *
 * Custom configuration via service properties will prevail on file properties. So you can override
 * one or more propertie from the file by filling correspondant properties
 *
 * Do not use Options object as it is still there only for backward compatibility
 *
 * For family specific options, you can specify them by using 'family.<familyName>.option.name'
 * for every family
 */
@Tags({ "rocksdb", "client"})
@CapabilityDescription("Implementation of RocksdbClientService for Rocksdb 5.4.0.")
public class Rocksdb_5_4_0_ClientService extends AbstractControllerService implements RocksdbClientService {

    protected RocksDB db;
    protected DBOptions dbOptions;
    protected List<String> familiesName = new ArrayList<>();
    protected Map<String, ColumnFamilyHandle> familiesHandler = new HashMap<>();
    protected Map<String, ColumnFamilyDescriptor> familiesDescriptor = new HashMap<>();
    static final protected String defaultFamily = "default";
    public static final String FAMILY_PREFIX = "family.";
    static final private Pattern dynamicFamiliesPropertiesPattern = Pattern.compile(
            "^" +
            FAMILY_PREFIX.replace(".", "\\.") +
                    "([^\\.]+)\\.(.+)$"
    );
    static final private List<String> familiesPropertiesSuffixe = Arrays.asList(
            OPTIMIZE_FOR_SMALL_DB.getName(),
            OPTIMIZE_FOR_POINT_LOOKUP.getName(),
            OPTIMIZE_LEVEL_STYLE_COMPACTION.getName(),
            OPTIMIZE_UNIVERSAL_STYLE_COMPACTION.getName(),
            FAMILY_KEY_COMPARATOR.getName(),
            MERGE_OPERATOR_NAME.getName(),
            WRITE_BUFFER_SIZE.getName(),
            MAX_WRITE_BUFFER_NUMBER.getName(),
            MIN_WRITE_BUFFER_NUMBER_TO_MERGE.getName(),
            USE_FIXED_LENGTH_PREFIX_EXTRACTOR.getName(),
            USE_CAPPED_PREFIX_EXTRACTOR.getName(),
            COMPRESSION_TYPE.getName(),
            COMPRESSION_PER_LEVEL.getName(),
            NUM_LEVELS.getName(),
            LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER.getName(),
            LEVEL_ZERO_SLOWDOWN_WRITES_TRIGGER.getName(),
            LEVEL_ZERO_STOP_WRITES_TRIGGER.getName(),
            TARGET_FILE_SIZE_BASE.getName(),
            TARGET_FILE_SIZE_MULTIPLIER.getName(),
            MAX_BYTES_FOR_LEVEL_BASE.getName(),
            LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES.getName(),
            MAX_BYTES_FOR_LEVEL_MULTIPLIER.getName(),
            MAX_COMPACTION_BYTES.getName(),
            ARENA_BLOCK_SIZE.getName(),
            DISABLE_AUTO_COMPACTIONS.getName(),
            COMPACTION_STYLE.getName(),
            MAX_TABLE_FILES_SIZE_FIFO.getName(),
            MAX_SEQUENTIAL_SKIP_IN_ITERATIONS.getName(),
            IN_PLACE_UPDATE_SUPPORT.getName(),
            IN_PLACE_UPDATE_NUM_LOCKS.getName(),
            MEM_TABLE_PREFIX_BLOOM_SIZE_RATIO.getName(),
            BLOOM_LOCALITY.getName(),
            MAX_SUCCESSIVE_MERGES.getName(),
            OPTIMIZE_FILTERS_FOR_HITS.getName(),
            MEMTABLE_HUGE_PAGE_SIZE.getName(),
            SOFT_PENDING_COMPACTION_BYTES_LIMIT.getName(),
            HARD_PENDING_COMPACTION_BYTES_LIMIT.getName(),
            LEVEL0_FILE_NUM_COMPACTION_TRIGGER.getName(),
            LEVEL0_SLOWDOWN_WRITES_TRIGGER.getName(),
            LEVEL0_STOP_WRITES_TRIGGER.getName(),
            PARANOID_FILE_CHECKS.getName(),
            MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN.getName(),
            REPORT_BG_IO_STATS.getName(),
            FORCE_CONSISTENCY_CHECKS.getName()
    );


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ROCKSDB_PATH);
        props.add(FAMILY_NAMES);
        props.add(OPTIMIZE_FOR_SMALL_DB);
        props.add(INCREASE_PARALLELISM);
        props.add(CREATE_IF_MISSING);
        props.add(CREATE_MISSING_COLUMN_FAMILIES);
        props.add(ERROR_IF_EXISTS);
        props.add(PARANOID_CHECKS);
        props.add(MAX_OPEN_FILES);
        props.add(MAX_FILE_OPENING_THREADS);
        props.add(MAX_TOTAL_WAL_SIZE);
        props.add(USE_FSYNC);
        props.add(DB_LOG_DIR);
        props.add(WAL_DIR);
        props.add(DELETE_OBSOLETE_FILES_PERIOD_MICROS);
        props.add(BASE_BACKGROUND_COMPACTIONS);
        props.add(MAX_BACKGROUND_COMPACTIONS);
        props.add(MAX_SUBCOMPACTIONS);
        props.add(MAX_BACKGROUND_FLUSHES);
        props.add(MAX_LOG_FILE_SIZE);
        props.add(LOG_FILE_TIME_TO_ROLL);
        props.add(KEEP_LOG_FILE_NUM);
        props.add(RECYCLE_LOG_FILE_NUM);
        props.add(MAX_MANIFEST_FILE_SIZE);
        props.add(TABLE_CACHE_NUMSHARDBITS);
        props.add(WAL_TTL_SECONDS);
        props.add(WAL_SIZE_LIMIT_MB);
        props.add(MANIFEST_PREALLOCATION_SIZE);
        props.add(USE_DIRECT_READS);
        props.add(USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION);
        props.add(ALLOW_F_ALLOCATE);
        props.add(ALLOW_MMAP_READS);
        props.add(ALLOW_MMAP_WRITES);
        props.add(IS_FD_CLOSE_ON_EXEC);
        props.add(STATS_DUMP_PERIOD_SEC);
        props.add(ADVISE_RANDOM_ON_OPEN);
        props.add(DB_WRITE_BUFFER_SIZE);
        props.add(NEW_TABLE_READER_FOR_COMPACTION_INPUTS);
        props.add(COMPACTION_READAHEAD_SIZE);
        props.add(RANDOM_ACCESS_MAX_BUFFER_SIZE);
        props.add(WRITABLE_FILE_MAX_BUFFER_SIZE);
        props.add(USE_ADAPTIVE_MUTEX);
        props.add(BYTES_PER_SYNC);
        props.add(WAL_BYTES_PER_SYNC);
        props.add(ENABLE_THREAD_TRACKING);
        props.add(DELAYED_WRITE_RATE);
        props.add(ALLOW_CONCURRENT_MEMTABLE_WRITE);
        props.add(ENABLE_WRITE_THREAD_ADAPTIVE_YIELD);
        props.add(WRITE_THREAD_MAX_YIELD_USEC);
        props.add(WRITE_THREAD_SLOW_YIELD_USEC);
        props.add(SKIP_STATS_UPDATE_ON_DB_OPEN);
        props.add(ALLOW_2_PC);
        props.add(FAIL_IF_OPTIONS_FILE_ERROR);
        props.add(DUMP_MALLOC_STATS);
        props.add(AVOID_FLUSH_DURING_RECOVERY);
        props.add(AVOID_FLUSH_DURING_SHUTDOWN);


        return Collections.unmodifiableList(props);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        Matcher matcher = dynamicFamiliesPropertiesPattern.matcher(propertyDescriptorName);
        if (matcher.matches()) {
            String familyName = matcher.group(1);
            String propertyName = matcher.group(2);
            if (familiesPropertiesSuffixe.contains(propertyName)) {
                PropertyDescriptor propDescriptor = getPropertyDescriptor(propertyName);
                return new PropertyDescriptor.Builder().fromPropertyDescriptor(propDescriptor)
                        .description("Specifies the value for '" + propertyName + "' for column family '" + familyName + "'.")
                        .displayName(propertyDescriptorName)
                        .name(propertyDescriptorName)
                        .dynamic(true)
                        .build();
            }
        }
        return null;
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        synchronized(this) {
            try {
                createDbClient(context);
            }catch (Exception e){
                throw new InitializationException(e);
            }
        }
    }

    /**
     * Instantiate RocksDb Client. This should be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an RocksDb client
     */
    protected void createDbClient(ControllerServiceInitializationContext context) throws ProcessException {
        //clean
        if (db != null) {
            return;
        }
        /*
            DBOptions
         */
        final DBOptions dbOptions = parseDbOptions(context);
        /*
            Families Descriptors
         */
        final List<ColumnFamilyDescriptor> familiesDescriptor = parseFamiliesDescriptor(context);
        /*
            Opening Db and filling up families handler
         */
        final String dbPath = context.getPropertyValue(ROCKSDB_PATH).asString();
        final String[] familiesName =  context.getPropertyValue(FAMILY_NAMES).asString().split(",");
        final List<ColumnFamilyHandle> familiesHandler = new ArrayList<>();
        try {
            db = RocksDB.open(dbOptions, dbPath, familiesDescriptor, familiesHandler);
        } catch (Exception e) {
            getLogger().error("Failed to create RocksDb client due to {}", new Object[]{e}, e);
            throw new RuntimeException(e);
        }
        if (db == null) {//RocksDB.open can return null
            getLogger().error("Failed to create RocksDb client for unknown reason");
            throw new ProcessException("Failed to create RocksDb client for unknown reason");
        }
        //initialize map of handlers
        for (int i=0; i<familiesName.length;i++) {
            String familyName = familiesName[i];
            this.familiesHandler.put(familyName, familiesHandler.get(i));
        }

    }

    /**
     * options currently supported are (see rocksDb documentation)
     *
     * optimize_for_small_db
     * increase_parallelism
     * create_if_missing
     * create_missing_column_families
     * error_if_exists
     * paranoid_checks
     * rate_limiter
     * max_open_files
     * max_file_opening_threads
     * max_total_wal_size
     * use_fsync
     * db_paths
     * db_log_dir
     * wal_dir
     * delete_obsolete_files_period_micros
     * base_background_compactions
     * max_background_compactions
     * max_subcompactions
     * max_background_flushes
     * max_log_file_size
     * log_file_time_to_roll
     * keep_log_file_num
     * recycle_log_file_num
     * max_manifest_file_size
     * table_cache_numshardbits
     * setWalTtlSeconds
     * setWalSizeLimitMB
     * setManifestPreallocationSize
     * setUseDirectReads
     * setUseDirectIoForFlushAndCompaction
     * setAllowFAllocate
     * setAllowMmapReads
     * setAllowMmapWrites
     * setIsFdCloseOnExec
     * setStatsDumpPeriodSec
     * setAdviseRandomOnOpen
     * setDbWriteBufferSize
     * setAccessHintOnCompactionStart
     * setNewTableReaderForCompactionInputs
     * setCompactionReadaheadSize
     * setRandomAccessMaxBufferSize
     * setWritableFileMaxBufferSize
     * setUseAdaptiveMutex
     * setBytesPerSync
     * setWalBytesPerSync
     * setEnableThreadTracking
     * setDelayedWriteRate
     * setAllowConcurrentMemtableWrite
     * setEnableWriteThreadAdaptiveYield
     * setWriteThreadMaxYieldUsec
     * setWriteThreadSlowYieldUsec
     * setSkipStatsUpdateOnDbOpen
     * setWalRecoveryMode
     * setAllow2pc
     * setRowCache
     * setFailIfOptionsFileError
     * setDumpMallocStats
     * setAvoidFlushDuringRecovery
     * setAvoidFlushDuringShutdown
     * @param context
     * @return
     */
    protected DBOptions parseDbOptions(ControllerServiceInitializationContext context)
    {
        DBOptions dbOptions = new DBOptions();
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB).isSet()) {
            if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB).asBoolean())
                dbOptions.optimizeForSmallDb();
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.INCREASE_PARALLELISM).isSet()) {
            int parallelism = context.getPropertyValue(Rocksdb_5_4_0_ClientService.INCREASE_PARALLELISM).asInteger();
            dbOptions.setIncreaseParallelism(parallelism);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING).isSet()) {
            boolean createIfMisssing = context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_IF_MISSING).asBoolean();
            dbOptions.setCreateIfMissing(createIfMisssing);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES).isSet()) {
            boolean createIfMisssingFamilies = context.getPropertyValue(Rocksdb_5_4_0_ClientService.CREATE_MISSING_COLUMN_FAMILIES).asBoolean();
            dbOptions.setCreateMissingColumnFamilies(createIfMisssingFamilies);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ERROR_IF_EXISTS).isSet()) {
            boolean createIfMisssingFamilies = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ERROR_IF_EXISTS).asBoolean();
            dbOptions.setErrorIfExists(createIfMisssingFamilies);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.PARANOID_CHECKS).isSet()) {
            boolean paranoidChecks = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ERROR_IF_EXISTS).asBoolean();
            dbOptions.setParanoidChecks(paranoidChecks);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.PARANOID_CHECKS).isSet()) {
            //TODO add possibility to use a custom Env
//            dbOptions.setEnv(<myEnv>);
//        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.PARANOID_CHECKS).isSet()) {
                //TODO
//                dbOptions.setRateLimiter(<myRateLimiter>);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_OPEN_FILES).isSet()) {
            int maxFiles = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_OPEN_FILES).asInteger();
            dbOptions.setMaxOpenFiles(maxFiles);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_FILE_OPENING_THREADS).isSet()) {
            int maxThreads = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_FILE_OPENING_THREADS).asInteger();
            dbOptions.setMaxFileOpeningThreads(maxThreads);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_TOTAL_WAL_SIZE).isSet()) {
            int maxWalSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_TOTAL_WAL_SIZE).asInteger();
            dbOptions.setMaxTotalWalSize(maxWalSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_FSYNC).isSet()) {
            boolean useFsync = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_FSYNC).asBoolean();
            dbOptions.setUseFsync(useFsync);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_PATHS).isSet()) {
//            //TODO parse field with regex
//            DbPath dbPath = new DbPath(path, size)
//            dbOptions.setDbPaths(dbPath);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_LOG_DIR).isSet()) {
            String dbLogDir = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_LOG_DIR).asString();
            dbOptions.setDbLogDir(dbLogDir);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_DIR).isSet()) {
            String walDir = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_DIR).asString();
            dbOptions.setWalDir(walDir);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELETE_OBSOLETE_FILES_PERIOD_MICROS).isSet()) {
            long period = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELETE_OBSOLETE_FILES_PERIOD_MICROS).asLong();
            dbOptions.setDeleteObsoleteFilesPeriodMicros(period);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.BASE_BACKGROUND_COMPACTIONS).isSet()) {
            int baseBackgroundCompactions = context.getPropertyValue(Rocksdb_5_4_0_ClientService.BASE_BACKGROUND_COMPACTIONS).asInteger();
            dbOptions.setBaseBackgroundCompactions(baseBackgroundCompactions);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_COMPACTIONS).isSet()) {
            int maxBackgroundCompaction = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_COMPACTIONS).asInteger();
            dbOptions.setMaxBackgroundCompactions(maxBackgroundCompaction);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_SUBCOMPACTIONS).isSet()) {
            int maxSubCompactions = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_SUBCOMPACTIONS).asInteger();
            dbOptions.setMaxSubcompactions(maxSubCompactions);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_FLUSHES).isSet()) {
            int maxBackgroundFlush = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_BACKGROUND_FLUSHES).asInteger();
            dbOptions.setMaxBackgroundFlushes(maxBackgroundFlush);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_LOG_FILE_SIZE).isSet()) {
            int maxLogFileSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_LOG_FILE_SIZE).asInteger();
            dbOptions.setMaxLogFileSize(maxLogFileSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.LOG_FILE_TIME_TO_ROLL).isSet()) {
            long logFileTimeToRoll = context.getPropertyValue(Rocksdb_5_4_0_ClientService.LOG_FILE_TIME_TO_ROLL).asLong();
            dbOptions.setLogFileTimeToRoll(logFileTimeToRoll);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.KEEP_LOG_FILE_NUM).isSet()) {
            long keepLogFileNum = context.getPropertyValue(Rocksdb_5_4_0_ClientService.KEEP_LOG_FILE_NUM).asLong();
            dbOptions.setKeepLogFileNum(keepLogFileNum);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.RECYCLE_LOG_FILE_NUM).isSet()) {
            long recycleLogFileNum = context.getPropertyValue(Rocksdb_5_4_0_ClientService.RECYCLE_LOG_FILE_NUM).asLong();
            dbOptions.setRecycleLogFileNum(recycleLogFileNum);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_MANIFEST_FILE_SIZE).isSet()) {
            long maxManifestFileSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MAX_MANIFEST_FILE_SIZE).asLong();
            dbOptions.setMaxManifestFileSize(maxManifestFileSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.TABLE_CACHE_NUMSHARDBITS).isSet()) {
            int tableCacheNumshardbits = context.getPropertyValue(Rocksdb_5_4_0_ClientService.TABLE_CACHE_NUMSHARDBITS).asInteger();
            dbOptions.setTableCacheNumshardbits(tableCacheNumshardbits);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_TTL_SECONDS).isSet()) {
            long walTtlSeconds = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_TTL_SECONDS).asLong();
            dbOptions.setWalTtlSeconds(walTtlSeconds);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_SIZE_LIMIT_MB).isSet()) {
            long walSizeLimitMb = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_SIZE_LIMIT_MB).asLong();
            dbOptions.setWalSizeLimitMB(walSizeLimitMb);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.MANIFEST_PREALLOCATION_SIZE).isSet()) {
            long manifestPreallocationSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.MANIFEST_PREALLOCATION_SIZE).asLong();
            dbOptions.setManifestPreallocationSize(manifestPreallocationSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_READS).isSet()) {
            boolean useDirectReads = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_READS).asBoolean();
            dbOptions.setUseDirectReads(useDirectReads);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION).isSet()) {
            boolean useDirectIoForFlushAndCompaction = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION).asBoolean();
            dbOptions.setUseDirectIoForFlushAndCompaction(useDirectIoForFlushAndCompaction);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_F_ALLOCATE).isSet()) {
            boolean allowFAllocate = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_F_ALLOCATE).asBoolean();
            dbOptions.setAllowFAllocate(allowFAllocate);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_READS).isSet()) {
            boolean allowMmapReads = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_READS).asBoolean();
            dbOptions.setAllowMmapReads(allowMmapReads);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_WRITES).isSet()) {
            boolean allowMmapWrites = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_MMAP_WRITES).asBoolean();
            dbOptions.setAllowMmapWrites(allowMmapWrites);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.IS_FD_CLOSE_ON_EXEC).isSet()) {
            boolean isFdCloseOnExec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.IS_FD_CLOSE_ON_EXEC).asBoolean();
            dbOptions.setIsFdCloseOnExec(isFdCloseOnExec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.STATS_DUMP_PERIOD_SEC).isSet()) {
            int statsDumpPeriodSec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.STATS_DUMP_PERIOD_SEC).asInteger();
            dbOptions.setStatsDumpPeriodSec(statsDumpPeriodSec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ADVISE_RANDOM_ON_OPEN).isSet()) {
            boolean adviseRandomOnOpen = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ADVISE_RANDOM_ON_OPEN).asBoolean();
            dbOptions.setAdviseRandomOnOpen(adviseRandomOnOpen);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_WRITE_BUFFER_SIZE).isSet()) {
            long dbWriteBufferSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DB_WRITE_BUFFER_SIZE).asLong();
            dbOptions.setDbWriteBufferSize(dbWriteBufferSize);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ACCESS_HINT_ON_COMPACTION_START).isSet()) {
//            AccessHint accessHint = new AccessHint();
//            //TODO
//            dbOptions.setAccessHintOnCompactionStart(accessHint);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.NEW_TABLE_READER_FOR_COMPACTION_INPUTS).isSet()) {
            boolean newTableReaderForCompactionInputs = context.getPropertyValue(Rocksdb_5_4_0_ClientService.NEW_TABLE_READER_FOR_COMPACTION_INPUTS).asBoolean();
            dbOptions.setNewTableReaderForCompactionInputs(newTableReaderForCompactionInputs);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.COMPACTION_READAHEAD_SIZE).isSet()) {
            long compactionReadaheadSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.COMPACTION_READAHEAD_SIZE).asLong();
            dbOptions.setCompactionReadaheadSize(compactionReadaheadSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.RANDOM_ACCESS_MAX_BUFFER_SIZE).isSet()) {
            long randomAccessmaxBufferSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.RANDOM_ACCESS_MAX_BUFFER_SIZE).asLong();
            dbOptions.setRandomAccessMaxBufferSize(randomAccessmaxBufferSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITABLE_FILE_MAX_BUFFER_SIZE).isSet()) {
            long writableFileMaxBufferSize = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITABLE_FILE_MAX_BUFFER_SIZE).asLong();
            dbOptions.setWritableFileMaxBufferSize(writableFileMaxBufferSize);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_ADAPTIVE_MUTEX).isSet()) {
            boolean useAdaptiveMutex = context.getPropertyValue(Rocksdb_5_4_0_ClientService.USE_ADAPTIVE_MUTEX).asBoolean();
            dbOptions.setUseAdaptiveMutex(useAdaptiveMutex);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.BYTES_PER_SYNC).isSet()) {
            long bytesPerSync = context.getPropertyValue(Rocksdb_5_4_0_ClientService.BYTES_PER_SYNC).asLong();
            dbOptions.setBytesPerSync(bytesPerSync);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_BYTES_PER_SYNC).isSet()) {
            long walBytesPerSync = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_BYTES_PER_SYNC).asLong();
            dbOptions.setWalBytesPerSync(walBytesPerSync);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_THREAD_TRACKING).isSet()) {
            boolean enableThreadTracking = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_THREAD_TRACKING).asBoolean();
            dbOptions.setEnableThreadTracking(enableThreadTracking);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELAYED_WRITE_RATE).isSet()) {
            long delayedWriteRate = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DELAYED_WRITE_RATE).asLong();
            dbOptions.setDelayedWriteRate(delayedWriteRate);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_CONCURRENT_MEMTABLE_WRITE).isSet()) {
            boolean delayedWriteRate = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_CONCURRENT_MEMTABLE_WRITE).asBoolean();
            dbOptions.setAllowConcurrentMemtableWrite(delayedWriteRate);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_WRITE_THREAD_ADAPTIVE_YIELD).isSet()) {
            boolean enableWriteThreadAdaptiveYield = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ENABLE_WRITE_THREAD_ADAPTIVE_YIELD).asBoolean();
            dbOptions.setEnableWriteThreadAdaptiveYield(enableWriteThreadAdaptiveYield);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_MAX_YIELD_USEC).isSet()) {
            long writeThreadMaxYieldUsec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_MAX_YIELD_USEC).asLong();
            dbOptions.setWriteThreadMaxYieldUsec(writeThreadMaxYieldUsec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_SLOW_YIELD_USEC).isSet()) {
            long writeThreadSlowYieldUsec = context.getPropertyValue(Rocksdb_5_4_0_ClientService.WRITE_THREAD_SLOW_YIELD_USEC).asLong();
            dbOptions.setWriteThreadSlowYieldUsec(writeThreadSlowYieldUsec);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.SKIP_STATS_UPDATE_ON_DB_OPEN).isSet()) {
            boolean skipStatsUpdateOnDbOpen = context.getPropertyValue(Rocksdb_5_4_0_ClientService.SKIP_STATS_UPDATE_ON_DB_OPEN).asBoolean();
            dbOptions.setSkipStatsUpdateOnDbOpen(skipStatsUpdateOnDbOpen);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.WAL_RECOVERY_MODE).isSet()) {
//            WALRecoveryMode mode = new WALRecoveryMode();
//            //TODO
//            dbOptions.setWalRecoveryMode(skipStatsUpdateOnDbOpen);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_2_PC).isSet()) {
            boolean allow2Pc = context.getPropertyValue(Rocksdb_5_4_0_ClientService.ALLOW_2_PC).asBoolean();
            dbOptions.setAllow2pc(allow2Pc);
        }
//        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.RowCache).isSet()) {
//            Cache cache = new LRUCache();
//            //TODO
//            dbOptions.setRowCache(cache);
//        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.FAIL_IF_OPTIONS_FILE_ERROR).isSet()) {
            boolean failIfOptionsFileError = context.getPropertyValue(Rocksdb_5_4_0_ClientService.FAIL_IF_OPTIONS_FILE_ERROR).asBoolean();
            dbOptions.setFailIfOptionsFileError(failIfOptionsFileError);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.DUMP_MALLOC_STATS).isSet()) {
            boolean dumpMallocStats = context.getPropertyValue(Rocksdb_5_4_0_ClientService.DUMP_MALLOC_STATS).asBoolean();
            dbOptions.setDumpMallocStats(dumpMallocStats);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_RECOVERY).isSet()) {
            boolean avoidFlushDuringRecovery = context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_RECOVERY).asBoolean();
            dbOptions.setAvoidFlushDuringRecovery(avoidFlushDuringRecovery);
        }
        if (context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_SHUTDOWN).isSet()) {
            boolean avoidFlushDuringShutdown = context.getPropertyValue(Rocksdb_5_4_0_ClientService.AVOID_FLUSH_DURING_SHUTDOWN).asBoolean();
            dbOptions.setAvoidFlushDuringShutdown(avoidFlushDuringShutdown);
        }
        return dbOptions;
    }

    /**
     * initialize {@link #familiesDescriptor}, {@link #familiesName}
     * @param context
     */
    protected  List<ColumnFamilyDescriptor> parseFamiliesDescriptor(ControllerServiceInitializationContext context)
    {
        /*
            initialize familiesName
         */
        final String[] familiesName =  context.getPropertyValue(FAMILY_NAMES).asString().split(",");
        this.familiesName = Arrays.asList(familiesName);
        /*
            initialize familiesDescriptor
        */
        final List<ColumnFamilyOptions> familiesOptions = parseFamiliesOptions(context);
        final List<ColumnFamilyDescriptor> familiesDescriptorList = new ArrayList<>();
        for (int i=0; i<familiesName.length;i++) {
            String familyName = familiesName[i];
            ColumnFamilyOptions familyOptions = familiesOptions.get(i);
            ColumnFamilyDescriptor fDescriptor = new ColumnFamilyDescriptor(familyName.getBytes() , familyOptions);
            familiesDescriptorList.add(fDescriptor);
            this.familiesDescriptor.put(familyName, fDescriptor);
        }
        return familiesDescriptorList;
    }
    protected List<ColumnFamilyOptions> parseFamiliesOptions(ControllerServiceInitializationContext context)
    {
        final List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
        final String[] familiesName =  context.getPropertyValue(FAMILY_NAMES).asString().split(",");

        for (int i=0; i<familiesName.length;i++) {
            final String familyPrefix = FAMILY_PREFIX + familiesName[i] + ".";
            final ColumnFamilyOptions familyOption = new ColumnFamilyOptions();
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB.getName()).isSet()) {
                if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_SMALL_DB.getName()).asBoolean())
                    familyOption.optimizeForSmallDb();
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_POINT_LOOKUP.getName()).isSet()) {
                long blockCacheSizeMb = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FOR_POINT_LOOKUP.getName()).asLong();
                familyOption.optimizeForPointLookup(blockCacheSizeMb);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_LEVEL_STYLE_COMPACTION.getName()).isSet()) {
                long memtableMemoryBudget = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_LEVEL_STYLE_COMPACTION.getName()).asLong();
                familyOption.optimizeLevelStyleCompaction(memtableMemoryBudget);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_UNIVERSAL_STYLE_COMPACTION.getName()).isSet()) {
                long memtableMemoryBudget = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_UNIVERSAL_STYLE_COMPACTION.getName()).asLong();
                familyOption.optimizeUniversalStyleCompaction(memtableMemoryBudget);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.FAMILY_KEY_COMPARATOR.getName()).isSet()) {
                String comparator = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.FAMILY_KEY_COMPARATOR.getName()).asString();
                if (BYTEWISE_COMPARATOR.getValue().equals(comparator)) {
                    familyOption.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
                } else if (REVERSE_BYTEWISE_COMPARATOR.getValue().equals(comparator)) {
                    familyOption.setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
                } else {
                    throw new RuntimeException("paranoid checks.'" + comparator + "' did not match any known key comparator.(property '" +
                            familyPrefix + Rocksdb_5_4_0_ClientService.FAMILY_KEY_COMPARATOR.getName() + "'");
                }
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MERGE_OPERATOR_NAME.getName()).isSet()) {
                String mergeOpName = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MERGE_OPERATOR_NAME.getName()).asString();
                familyOption.setMergeOperatorName(mergeOpName);
            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MERGE_OPERATOR.getName()).isSet()) {
//                String mergeOpName = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MERGE_OPERATOR.getName()).asString();
                //TODO implement custom merge operator
//                familyOption.setMergeOperator();
//            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPACTION_FILTER.getName()).isSet()) {
//                //TODO implement custom merge operator
//                String mergeOpName = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPACTION_FILTER.getName()).asString();
//            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.WRITE_BUFFER_SIZE.getName()).isSet()) {
                long writeBufferSize = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.WRITE_BUFFER_SIZE.getName()).asLong();
                familyOption.setWriteBufferSize(writeBufferSize);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_WRITE_BUFFER_NUMBER.getName()).isSet()) {
                int maxWriteBufferNumber = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_WRITE_BUFFER_NUMBER.getName()).asInteger();
                familyOption.setMaxWriteBufferNumber(maxWriteBufferNumber);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.getName()).isSet()) {
                int minWriteBufferToMergeNumber = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.getName()).asInteger();
                familyOption.setMinWriteBufferNumberToMerge(minWriteBufferToMergeNumber);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_FIXED_LENGTH_PREFIX_EXTRACTOR.getName()).isSet()) {
                int fixedLength = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_FIXED_LENGTH_PREFIX_EXTRACTOR.getName()).asInteger();
                familyOption.useFixedLengthPrefixExtractor(fixedLength);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_CAPPED_PREFIX_EXTRACTOR.getName()).isSet()) {
                int capped = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_CAPPED_PREFIX_EXTRACTOR.getName()).asInteger();
                familyOption.useCappedPrefixExtractor(capped);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPRESSION_TYPE.getName()).isSet()) {
                String compressionType = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPRESSION_TYPE.getName()).asString();
                familyOption.setCompressionType(CompressionType.getCompressionType(compressionType));
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPRESSION_PER_LEVEL.getName()).isSet()) {
                String[] compressionTypes = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPRESSION_PER_LEVEL.getName()).asString().split(",");
                List<CompressionType> cTypes = new ArrayList<>();
                for (int j=0; j < compressionTypes.length; j++) {
                    cTypes.add(CompressionType.getCompressionType(compressionTypes[i]));
                }
                familyOption.setCompressionPerLevel(cTypes);
            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_CAPPED_PREFIX_EXTRACTOR.getName()).isSet()) {
//                //TODO implement BOTTOMMOST_COMPRESSION_TYPE
//                int capped = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_CAPPED_PREFIX_EXTRACTOR.getName()).asInteger();
//                familyOption.setBottommostCompressionType(capped);
//            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_CAPPED_PREFIX_EXTRACTOR.getName()).isSet()) {
//                //TODO support compression options
//                int capped = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.USE_CAPPED_PREFIX_EXTRACTOR.getName()).asInteger();
//                familyOption.setCompressionOptions(CompressionOptions);
//            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.NUM_LEVELS.getName()).isSet()) {
                int numberOfLEvels = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.NUM_LEVELS.getName()).asInteger();
                familyOption.setNumLevels(numberOfLEvels);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER.getName()).isSet()) {
                int numLevelZeroFileCompactionTrigger = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER.getName()).asInteger();
                familyOption.setLevelZeroFileNumCompactionTrigger(numLevelZeroFileCompactionTrigger);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_ZERO_SLOWDOWN_WRITES_TRIGGER.getName()).isSet()) {
                int numLevelZeroSlowdownWritesTrigger = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_ZERO_SLOWDOWN_WRITES_TRIGGER.getName()).asInteger();
                familyOption.setLevelZeroSlowdownWritesTrigger(numLevelZeroSlowdownWritesTrigger);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_ZERO_STOP_WRITES_TRIGGER.getName()).isSet()) {
                int numLevelZeroStopWritesTrigger = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_ZERO_STOP_WRITES_TRIGGER.getName()).asInteger();
                familyOption.setLevelZeroStopWritesTrigger(numLevelZeroStopWritesTrigger);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.TARGET_FILE_SIZE_BASE.getName()).isSet()) {
                int targetFileSizeBase = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.TARGET_FILE_SIZE_BASE.getName()).asInteger();
                familyOption.setTargetFileSizeBase(targetFileSizeBase);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.TARGET_FILE_SIZE_MULTIPLIER.getName()).isSet()) {
                int targetFileSizeMultiplier = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.TARGET_FILE_SIZE_MULTIPLIER.getName()).asInteger();
                familyOption.setTargetFileSizeMultiplier(targetFileSizeMultiplier);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_BYTES_FOR_LEVEL_BASE.getName()).isSet()) {
                long maxBytesForLevelBase = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_BYTES_FOR_LEVEL_BASE.getName()).asLong();
                familyOption.setMaxBytesForLevelBase(maxBytesForLevelBase);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES.getName()).isSet()) {
                boolean dynamicLevelBytes = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES.getName()).asBoolean();
                familyOption.setLevelCompactionDynamicLevelBytes(dynamicLevelBytes);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_BYTES_FOR_LEVEL_MULTIPLIER.getName()).isSet()) {
                double maxBytesForLevelMultiplier = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_BYTES_FOR_LEVEL_MULTIPLIER.getName()).asDouble();
                familyOption.setMaxBytesForLevelMultiplier(maxBytesForLevelMultiplier);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_COMPACTION_BYTES.getName()).isSet()) {
                long maxCompactionBytes = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_COMPACTION_BYTES.getName()).asLong();
                familyOption.setMaxCompactionBytes(maxCompactionBytes);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.ARENA_BLOCK_SIZE.getName()).isSet()) {
                long arenaBlockSize = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.ARENA_BLOCK_SIZE.getName()).asLong();
                familyOption.setArenaBlockSize(arenaBlockSize);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.DISABLE_AUTO_COMPACTIONS.getName()).isSet()) {
                boolean autoCompaction = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.DISABLE_AUTO_COMPACTIONS.getName()).asBoolean();
                familyOption.setDisableAutoCompactions(autoCompaction);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPACTION_STYLE.getName()).isSet()) {
                String compaction = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.COMPACTION_STYLE.getName()).asString();
                familyOption.setCompactionStyle(CompactionStyle.valueOf(compaction));
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_TABLE_FILES_SIZE_FIFO.getName()).isSet()) {
                long maxTableFileSizeFifo = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_TABLE_FILES_SIZE_FIFO.getName()).asLong();
                familyOption.setMaxTableFilesSizeFIFO(maxTableFileSizeFifo);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SEQUENTIAL_SKIP_IN_ITERATIONS.getName()).isSet()) {
                long maxSequentialSkipInIterations = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SEQUENTIAL_SKIP_IN_ITERATIONS.getName()).asLong();
                familyOption.setMaxSequentialSkipInIterations(maxSequentialSkipInIterations);
            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SEQUENTIAL_SKIP_IN_ITERATIONS.getName()).isSet()) {
//                long maxSequentialSkipInIterations = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SEQUENTIAL_SKIP_IN_ITERATIONS.getName()).asLong();
//                //TODO support memTableConfig
//                familyOption.setMemTableConfig(maxSequentialSkipInIterations);
//            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SEQUENTIAL_SKIP_IN_ITERATIONS.getName()).isSet()) {
//                long maxSequentialSkipInIterations = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SEQUENTIAL_SKIP_IN_ITERATIONS.getName()).asLong();
//                //                //TODO support TableFormatConfig
//                familyOption.setTableFormatConfig();
//            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.IN_PLACE_UPDATE_SUPPORT.getName()).isSet()) {
                boolean inPlaceUpdateSupport = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.IN_PLACE_UPDATE_SUPPORT.getName()).asBoolean();
                familyOption.setInplaceUpdateSupport(inPlaceUpdateSupport);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.IN_PLACE_UPDATE_NUM_LOCKS.getName()).isSet()) {
                long inPlaceUpdateNumLocks = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.IN_PLACE_UPDATE_NUM_LOCKS.getName()).asLong();
                familyOption.setInplaceUpdateNumLocks(inPlaceUpdateNumLocks);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MEM_TABLE_PREFIX_BLOOM_SIZE_RATIO.getName()).isSet()) {
                double prefixBloomSizeRatio = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MEM_TABLE_PREFIX_BLOOM_SIZE_RATIO.getName()).asDouble();
                familyOption.setMemtablePrefixBloomSizeRatio(prefixBloomSizeRatio);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.BLOOM_LOCALITY.getName()).isSet()) {
                int blomLocality = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.BLOOM_LOCALITY.getName()).asInteger();
                familyOption.setBloomLocality(blomLocality);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SUCCESSIVE_MERGES.getName()).isSet()) {
                long maxSuccessiveMerges = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_SUCCESSIVE_MERGES.getName()).asLong();
                familyOption.setMaxSuccessiveMerges(maxSuccessiveMerges);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FILTERS_FOR_HITS.getName()).isSet()) {
                boolean optimizeFiltersForHits = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.OPTIMIZE_FILTERS_FOR_HITS.getName()).asBoolean();
                familyOption.setOptimizeFiltersForHits(optimizeFiltersForHits);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MEMTABLE_HUGE_PAGE_SIZE.getName()).isSet()) {
                long memTableHugePageSize = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MEMTABLE_HUGE_PAGE_SIZE.getName()).asLong();
                familyOption.setMemtableHugePageSize(memTableHugePageSize);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.SOFT_PENDING_COMPACTION_BYTES_LIMIT.getName()).isSet()) {
                long softPendingCompactionBytesLimit = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.SOFT_PENDING_COMPACTION_BYTES_LIMIT.getName()).asLong();
                familyOption.setSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.HARD_PENDING_COMPACTION_BYTES_LIMIT.getName()).isSet()) {
                long hardPendingCompactionBytesLimit = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.SOFT_PENDING_COMPACTION_BYTES_LIMIT.getName()).asLong();
                familyOption.setHardPendingCompactionBytesLimit(hardPendingCompactionBytesLimit);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL0_FILE_NUM_COMPACTION_TRIGGER.getName()).isSet()) {
                int level0FileNumCompactionTrigger = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL0_FILE_NUM_COMPACTION_TRIGGER.getName()).asInteger();
                familyOption.setLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL0_SLOWDOWN_WRITES_TRIGGER.getName()).isSet()) {
                int level0SlowdownWritesTrigger = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL0_SLOWDOWN_WRITES_TRIGGER.getName()).asInteger();
                familyOption.setLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL0_STOP_WRITES_TRIGGER.getName()).isSet()) {
                int level0SlopWritesTrigger = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.LEVEL0_STOP_WRITES_TRIGGER.getName()).asInteger();
                familyOption.setLevel0StopWritesTrigger(level0SlopWritesTrigger);
            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_BYTES_FOR_LEVEL_MULTIPLIER_ADDITIONAL.getName()).isSet()) {
//                //TODO manage this (as string ???)
//                int[] maxBytesForLevelMultiplierAdditional = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_BYTES_FOR_LEVEL_MULTIPLIER_ADDITIONAL.getName()).asDouble();
//                familyOption.setMaxBytesForLevelMultiplierAdditional(maxBytesForLevelMultiplierAdditional);
//            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.PARANOID_FILE_CHECKS.getName()).isSet()) {
                boolean paranoidChecks = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.PARANOID_FILE_CHECKS.getName()).asBoolean();
                familyOption.setParanoidFileChecks(paranoidChecks);
            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN.getName()).isSet()) {
                int maxWriteBufferNumberToMaintain = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN.getName()).asInteger();
                familyOption.setMaxWriteBufferNumberToMaintain(maxWriteBufferNumberToMaintain);
            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN.getName()).isSet()) {
//                int maxWriteBufferNumberToMaintain = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN.getName()).asInteger();
//                //TODO manage CompactionPriority
//                familyOption.setCompactionPriority(maxWriteBufferNumberToMaintain);
//            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.REPORT_BG_IO_STATS.getName()).isSet()) {
                boolean reportBgIoStats = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.REPORT_BG_IO_STATS.getName()).asBoolean();
                familyOption.setReportBgIoStats(reportBgIoStats);
            }
//            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.REPORT_BG_IO_STATS.getName()).isSet()) {
//                boolean reportBgIoStats = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.REPORT_BG_IO_STATS.getName()).asBoolean();
//                //TODO manage CompactionOptionsUniversal
//                familyOption.setCompactionOptionsUniversal(reportBgIoStats);
//            }
            //            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.REPORT_BG_IO_STATS.getName()).isSet()) {
//                boolean reportBgIoStats = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.REPORT_BG_IO_STATS.getName()).asBoolean();
//                //TODO manage CompactionOptionsFIFO
//                familyOption.setCompactionOptionsUniversal(reportBgIoStats);
//            }
            if (context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.FORCE_CONSISTENCY_CHECKS.getName()).isSet()) {
                boolean forceConsistencyChecks = context.getPropertyValue(familyPrefix + Rocksdb_5_4_0_ClientService.FORCE_CONSISTENCY_CHECKS.getName()).asBoolean();
                familyOption.setForceConsistencyChecks(forceConsistencyChecks);
            }
            familyOptions.add(familyOption);
        }
        return familyOptions;
    }

    @OnDisabled
    public void shutdown() {
        /*
        from rocksdb documentation:
        Even if ColumnFamilyHandle is pointing to a dropped Column Family, you can continue using it.
        The data is actually deleted only after you delete all outstanding ColumnFamilyHandles.
         */
        if (familiesHandler != null && !familiesHandler.isEmpty()) {
            for (ColumnFamilyHandle fHandle : familiesHandler.values()) {
                fHandle.close();
            }
        }
        if (db != null) {
            db.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
    }

    /**
     *
     * @param puts a list of put mutations
     * @throws RocksDBException
     */
    @Override
    public void multiPut(Collection<ValuePutRequest> puts) throws RocksDBException {
        for (ValuePutRequest putR: puts) {
            put(putR);
        }
    }

    /**
     *
     * @param put a put mutation
     * @throws RocksDBException
     * @throws IllegalArgumentException
     * @throws NullPointerException if put is null
     */
    @Override
    public void put(ValuePutRequest put) throws RocksDBException, IllegalArgumentException {
        String family = put.getFamily();
        byte[] key = put.getKey();
        byte[] value = put.getValue();
        WriteOptions wOptions = put.getwOptions();
        if (key==null || value==null) {
            throw new IllegalArgumentException("key and value can not be null");
        }
        if (wOptions == null) {
            wOptions = new WriteOptions();
        }
        if (family == null) {
            family = defaultFamily;
        }
        put(family, key, value, wOptions);
    }

    /**
     *
     * @param familyName family to put data in
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @throws RocksDBException*
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void put(String familyName, byte[] key, byte[] value) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        db.put(fHandle, key, value);
    }

    /**
     *
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @throws RocksDBException
     * @throws NullPointerException if a parameter is null
     */
    @Override
    public void put(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    /**
     *
     * @param familyName family to put data in
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @param writeOptions
     * @throws RocksDBException
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void put(String familyName, byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        db.put(fHandle, writeOptions, key, value);
    }

    /**
     *
     * @param key the key of the value to store
     * @param value the value to store in the specified family
     * @param writeOptions
     * @throws RocksDBException
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public void put(byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException {
        db.put(writeOptions, key, value);
    }

    /**
     *
     * @param getRequests a list of single get to do
     * @return a list of response
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    @Override
    public Collection<GetResponse> multiGet(Collection<GetRequest> getRequests) throws RocksDBException {
        Collection<GetResponse> responses = new ArrayList<>();
        for (GetRequest getR: getRequests) {
            responses.add(get(getR));
        }
        return  responses;
    }

    /**
     *
     * @param getRequest a single get request
     * @return a single getResponse
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public GetResponse get(GetRequest getRequest) throws RocksDBException {
        String family = getRequest.getFamily();
        final byte[] key = getRequest.getKey();
        ReadOptions rOptions = getRequest.getReadOption();
        if (key==null) {
            throw new IllegalArgumentException("key can not be null");
        }
        if (rOptions == null) {
            rOptions = new ReadOptions();
        }
        if (family == null) {
            family = defaultFamily;
        }
        byte[] value = get(family, key, rOptions);
        GetResponse resp = new GetResponse();
        resp.setFamily(family);
        resp.setKey(key);
        resp.setValue(value);
        return  resp;
    }

    /**
     *
     * @param key the key to retrieve the value in the 'default' family
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public byte[] get(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    /**
     *
     * @param key the key to retrieve the value in the 'default' family
     * @param rOption
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public byte[] get(byte[] key, ReadOptions rOption) throws RocksDBException {
        return db.get(rOption, key);
    }

    /**
     *
     * @param familyName the family where to get the value from
     * @param key the key to retrieve the value in the family
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public byte[] get(String familyName, byte[] key) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        return db.get(fHandle, key);
    }

    /**
     *
     * @param familyName the family where to get the value from
     * @param key the key to retrieve the value in the family
     * @param rOption
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public byte[] get(String familyName, byte[] key, ReadOptions rOption) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        return db.get(fHandle, rOption, key);
    }

    /**
     *
     * @param deleteRequests a list of value to delete
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     */
    @Override
    public Collection<DeleteResponse> multiDelete(Collection<DeleteRequest> deleteRequests) throws RocksDBException {
        Collection<DeleteResponse> responses = new ArrayList<>();
        for (DeleteRequest deleteR: deleteRequests) {
            responses.add(delete(deleteR));
        }
        return responses;
    }

    /**
     *
     * @param deleteRequest a value to delete
     * @return
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public DeleteResponse delete(DeleteRequest deleteRequest) throws RocksDBException {
        String family = deleteRequest.getFamily();
        final byte[] key = deleteRequest.getKey();
        WriteOptions wOptions = deleteRequest.getWriteOptions();
        if (key==null) {
            throw new IllegalArgumentException("key can not be null");
        }
        if (wOptions == null) {
            wOptions = new WriteOptions();
        }
        if (family == null) {
            family = defaultFamily;
        }
        delete(family, key, wOptions);
        DeleteResponse resp = new DeleteResponse();
        resp.setFamily(family);
        resp.setKey(key);
        return  resp;
    }

    /**
     *
     * @param key a key to delete with his value in 'default' family
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public void delete(byte[] key) throws RocksDBException {
        db.delete(key);
    }

    /**
     *
     * @param key  a key to delete with his value in 'default' family
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public void delete(byte[] key, WriteOptions wOption) throws RocksDBException {
        db.delete(wOption, key);
    }

    /**
     *
     * @param familyName the family to do the delete
     * @param key  a key to delete with his value in family
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void delete(String familyName, byte[] key) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        db.delete(fHandle, key);
    }

    /**
     *
     * @param familyName the family to do the delete
     * @param key a key to delete with his value in family
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void delete(String familyName, byte[] key, WriteOptions wOption) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        db.delete(fHandle, wOption, key);
    }

    /**
     *
     * @param keyStart first key to delete data from in 'default' family (included)
     * @param keyEnd last key to delete data from in 'default' family (excluded)
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public void deleteRange(byte[] keyStart, byte[] keyEnd) throws RocksDBException {
        db.deleteRange(keyStart, keyEnd);
    }

    /**
     *
     * @param keyStart first key to delete data from in 'default' family (included)
     * @param keyEnd last key to delete data from in 'default' family (excluded)
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public void deleteRange(byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException {
        db.deleteRange(wOption, keyStart, keyEnd);
    }

    /**
     *
     * @param familyName family name to delete data from
     * @param keyStart first key to delete data from in family (included)
     * @param keyEnd last key to delete data from in family (excluded)
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        db.deleteRange(fHandle, keyStart, keyEnd);
    }

    /**
     *
     * @param familyName family name to delete data from
     * @param keyStart first key to delete data from in family (included)
     * @param keyEnd last key to delete data from in family (excluded)
     * @param wOption
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        db.deleteRange(fHandle, wOption, keyStart, keyEnd);
    }

    /**
     *
     * Scans the 'default' family passing each result to the provided handler.
     *
     * @param handler  a handler to process iterators from rocksdb
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     */
    @Override
    public void scan(RocksIteratorHandler handler) throws RocksDBException {
        handler.handle(db.newIterator());
    }

    /**
     *
     * Scans the given family passing the result to the provided handler.
     *
     * @param familyName the column family to scan over
     * @param handler  a handler to process iterators from rocksdb
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void scan(String familyName, RocksIteratorHandler handler) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        handler.handle(db.newIterator(fHandle));
    }

    /**
     *
     * Scans the given family passes the result to the handler.
     *
     * @param familyName the column family to scan over
     * @param rOptions readOptions
     * @param handler a handler to process iterators from rocksdb
     * @throws RocksDBException thrown when there are communication errors with RocksDb
     * @throws NullPointerException if a parameter is null (beside familyName).
     * @throws IllegalArgumentException if familyName is not known in the db or null.
     */
    @Override
    public void scan(String familyName, ReadOptions rOptions, RocksIteratorHandler handler) throws RocksDBException {
        ColumnFamilyHandle fHandle = getFamilyHandle(familyName);
        handler.handle(db.newIterator(fHandle, rOptions));
    }

    private ColumnFamilyHandle getFamilyHandle(String familyName) throws IllegalArgumentException {
        ColumnFamilyHandle fHandle = familiesHandler.get(familyName);
        if (fHandle==null) {
            throw new IllegalArgumentException("family '" + familyName +
                    "' does not exist. Please specify it in Db options with option '" + FAMILY_NAMES.getName() + "'");
        }
        return fHandle;
    }
}
