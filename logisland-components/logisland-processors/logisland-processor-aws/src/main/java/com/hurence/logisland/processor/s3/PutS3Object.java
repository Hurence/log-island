package com.hurence.logisland.processor.s3;

import java.io.*;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Tag;
import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.behavior.ReadsAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttributes;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.state.DataUnit;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;


@SeeAlso({FetchS3Object.class, DeleteS3Object.class, ListS3.class})
@Tags({"Amazon", "S3", "AWS", "Archive", "Put"})
@CapabilityDescription("Puts Fields to an Amazon S3 Bucket.\n" +
        "The upload uses either the PutS3Object method or the PutS3MultipartUpload method.  The PutS3Object method " +
        "sends the file in a single synchronous call, but it has a 5GB size limit.  Larger files are sent using the " +
        "PutS3MultipartUpload method.  This multipart process " +
        "saves state after each step so that a large upload can be resumed with minimal loss if the processor or " +
        "cluster is stopped and restarted.\n" +
        "A multipart upload consists of three steps:\n" +
        "  1) initiate upload,\n" +
        "  2) upload the parts, and\n" +
        "  3) complete the upload.\n" +
        "For multipart uploads, the processor saves state locally tracking the upload ID and parts uploaded, which " +
        "must both be provided to complete the upload.\n" +
        "The AWS libraries select an endpoint URL based on the AWS region, but this can be overridden with the " +
        "'Endpoint Override URL' property for use with other S3-compatible endpoints.\n" +
        "The S3 API specifies that the maximum file size for a PutS3Object upload is 5GB. It also requires that " +
        "parts in a multipart upload must be at least 5MB in size, except for the last part.  These limits " +
        "establish the bounds for the Multipart Upload Threshold and Part Size properties.")
@DynamicProperty(name = "The name of a User-Defined Metadata field to add to the S3 Object",
        value = "The value of a User-Defined Metadata field to add to the S3 Object",
        description = "Allows user-defined metadata to be added to the S3 object as key/value pairs"
        /*expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES*/) // TODO
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the S3 object")
@WritesAttributes({
        @WritesAttribute(attribute = "s3.bucket", description = "The S3 bucket where the Object was put in S3"),
        @WritesAttribute(attribute = "s3.key", description = "The S3 key within where the Object was put in S3"),
        @WritesAttribute(attribute = "s3.contenttype", description = "The S3 content type of the S3 Object that put in S3"),
        @WritesAttribute(attribute = "s3.version", description = "The version of the S3 Object that was put to S3"),
        @WritesAttribute(attribute = "s3.etag", description = "The ETag of the S3 Object"),
        @WritesAttribute(attribute = "s3.uploadId", description = "The uploadId used to upload the Object to S3"),
        @WritesAttribute(attribute = "s3.expiration", description = "A human-readable form of the expiration date of " +
                "the S3 object, if one is set"),
        @WritesAttribute(attribute = "s3.sseAlgorithm", description = "The server side encryption algorithm of the object"),
        @WritesAttribute(attribute = "s3.usermetadata", description = "A human-readable form of the User Metadata of " +
                "the S3 object, if any was set"),
        @WritesAttribute(attribute = "s3.encryptionStrategy", description = "The name of the encryption strategy, if any was set"),})
public class PutS3Object extends AbstractS3Processor {

    public static final long MIN_S3_PART_SIZE = 50L * 1024L * 1024L;
    public static final long MAX_S3_PUTOBJECT_SIZE = 5L * 1024L * 1024L * 1024L;
    public static final String PERSISTENCE_ROOT = "conf/state/";
    public static final String NO_SERVER_SIDE_ENCRYPTION = "None";

    public static final PropertyDescriptor EXPIRATION_RULE_ID = new PropertyDescriptor.Builder()
            .name("Expiration Time Rule")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Content Type")
            .displayName("Content Type")
            .description("Sets the Content-Type HTTP header indicating the type of content stored in the associated " +
                    "object. The value of this header is a standard MIME type.\n" +
                    "AWS S3 Java client will attempt to determine the correct content type if one hasn't been set" +
                    " yet. Users are responsible for ensuring a suitable content type is set when uploading streams. If " +
                    "no content type is provided and cannot be determined by the filename, the default content type " +
                    "\"application/octet-stream\" will be used.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STORAGE_CLASS = new PropertyDescriptor.Builder()
            .name("Storage Class")
            .required(true)
            .allowableValues(StorageClass.Standard.name(), StorageClass.IntelligentTiering.name(), StorageClass.StandardInfrequentAccess.name(),
                    StorageClass.OneZoneInfrequentAccess.name(), StorageClass.Glacier.name(), StorageClass.DeepArchive.name(), StorageClass.ReducedRedundancy.name())
            .defaultValue(StorageClass.Standard.name())
            .build();

    public static final PropertyDescriptor MULTIPART_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Multipart Threshold")
            .description("Specifies the file size threshold for switch from the PutS3Object API to the " +
                    "PutS3MultipartUpload API.  Flow files bigger than this limit will be sent using the stateful " +
                    "multipart process.\n" +
                    "The valid range is 50MB to 5GB.")
            .required(true)
            .defaultValue("5 GB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(MIN_S3_PART_SIZE, MAX_S3_PUTOBJECT_SIZE))
            .build();

    public static final PropertyDescriptor MULTIPART_PART_SIZE = new PropertyDescriptor.Builder()
            .name("Multipart Part Size")
            .description("Specifies the part size for use when the PutS3Multipart Upload API is used.\n" +
                    "Flow files will be broken into chunks of this size for the upload process, but the last part " +
                    "sent can be smaller since it is not padded.\n" +
                    "The valid range is 50MB to 5GB.")
            .required(true)
            .defaultValue("5 GB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(MIN_S3_PART_SIZE, MAX_S3_PUTOBJECT_SIZE))
            .build();

    public static final PropertyDescriptor MULTIPART_S3_AGEOFF_INTERVAL = new PropertyDescriptor.Builder()
            .name("Multipart Upload AgeOff Interval")
            .description("Specifies the interval at which existing multipart uploads in AWS S3 will be evaluated " +
                    "for ageoff.  When processor is triggered it will initiate the ageoff evaluation if this interval has been " +
                    "exceeded.")
            .required(true)
            .defaultValue("60 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MULTIPART_S3_MAX_AGE = new PropertyDescriptor.Builder()
            .name("Multipart Upload Max Age Threshold")
            .description("Specifies the maximum age for existing multipart uploads in AWS S3.  When the ageoff " +
                    "process occurs, any upload older than this threshold will be aborted.")
            .required(true)
            .defaultValue("7 days")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor SERVER_SIDE_ENCRYPTION = new PropertyDescriptor.Builder()
            .name("server-side-encryption")
            .displayName("Server Side Encryption")
            .description("Specifies the algorithm used for server side encryption.")
            .required(true)
            .allowableValues(NO_SERVER_SIDE_ENCRYPTION, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
            .defaultValue(NO_SERVER_SIDE_ENCRYPTION)
            .build();

    public static final PropertyDescriptor OBJECT_TAGS_PREFIX = new PropertyDescriptor.Builder()
            .name("s3-object-tags-prefix")
            .displayName("Object Tags Prefix")
            .description("Specifies the prefix which would be scanned against the incoming FlowFile's attributes and the matching attribute's " +
                    "name and value would be considered as the outgoing S3 object's Tag name and Tag value respectively. For Ex: If the " +
                    "incoming FlowFile carries the attributes tagS3country, tagS3PII, the tag prefix to be specified would be 'tagS3'")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor REMOVE_TAG_PREFIX = new PropertyDescriptor.Builder()
            .name("s3-object-remove-tags-prefix")
            .displayName("Remove Tag Prefix")
            .description("If set to 'True', the value provided for '" + OBJECT_TAGS_PREFIX.getDisplayName() + "' will be removed from " +
                    "the attribute(s) and then considered as the Tag name. For ex: If the incoming FlowFile carries the attributes tagS3country, " +
                    "tagS3PII and the prefix is set to 'tagS3' then the corresponding tag values would be 'country' and 'PII'")
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KEY_FEILD, BUCKET_FIELD, CONTENT_TYPE, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, OBJECT_TAGS_PREFIX, REMOVE_TAG_PREFIX,
                    STORAGE_CLASS, REGION, TIMEOUT, EXPIRATION_RULE_ID, FULL_CONTROL_USER_LIST, READ_USER_LIST, WRITE_USER_LIST, READ_ACL_LIST, WRITE_ACL_LIST, OWNER,
                    CANNED_ACL, SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE, SIGNER_OVERRIDE, MULTIPART_THRESHOLD, MULTIPART_PART_SIZE, MULTIPART_S3_AGEOFF_INTERVAL,
                    MULTIPART_S3_MAX_AGE, SERVER_SIDE_ENCRYPTION, ENCRYPTION_SERVICE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST,
                    PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD));

    final static String S3_BUCKET_KEY = "s3.bucket";
    final static String S3_OBJECT_KEY = "s3.key";
    final static String S3_CONTENT_TYPE = "s3.contenttype";
    final static String S3_UPLOAD_ID_ATTR_KEY = "s3.uploadId";
    final static String S3_VERSION_ATTR_KEY = "s3.version";
    final static String S3_ETAG_ATTR_KEY = "s3.etag";
    final static String S3_EXPIRATION_ATTR_KEY = "s3.expiration";
    final static String S3_STORAGECLASS_ATTR_KEY = "s3.storeClass";
    final static String S3_STORAGECLASS_META_KEY = "x-amz-storage-class";
    final static String S3_USERMETA_ATTR_KEY = "s3.usermetadata";
    final static String S3_API_METHOD_ATTR_KEY = "s3.apimethod";
    final static String S3_API_METHOD_PUTOBJECT = "putobject";
    final static String S3_API_METHOD_MULTIPARTUPLOAD = "multipartupload";
    final static String S3_SSE_ALGORITHM = "s3.sseAlgorithm";
    final static String S3_ENCRYPTION_STRATEGY = "s3.encryptionStrategy";


    final static String S3_PROCESS_UNSCHEDULED_MESSAGE = "Processor unscheduled, stopping upload";

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    protected File getPersistenceFile() {
        return new File(PERSISTENCE_ROOT + getIdentifier());
    }

    protected boolean localUploadExistsInS3(final AmazonS3Client s3, final String bucket, final MultipartState localState) {
        ListMultipartUploadsRequest listRequest = new ListMultipartUploadsRequest(bucket);
        MultipartUploadListing listing = s3.listMultipartUploads(listRequest);

        for (MultipartUpload upload : listing.getMultipartUploads()) {
            if (upload.getUploadId().equals(localState.getUploadId())) {
                return true;
            }
        }
        return false;
    }

    protected synchronized MultipartState getLocalStateIfInS3(final AmazonS3Client s3, final String bucket,
                                                              final String s3ObjectKey) throws IOException {
        MultipartState currState = getLocalState(s3ObjectKey);
        if (currState == null) {
            return null;
        }
        if (localUploadExistsInS3(s3, bucket, currState)) {
            getLogger().info("Local state for {} loaded with uploadId {} and {} partETags",
                    new Object[]{s3ObjectKey, currState.getUploadId(), currState.getPartETags().size()});
            return currState;
        } else {
            getLogger().info("Local state for {} with uploadId {} does not exist in S3, deleting local state",
                    new Object[]{s3ObjectKey, currState.getUploadId()});
            persistLocalState(s3ObjectKey, null);
            return null;
        }
    }

    protected synchronized MultipartState getLocalState(final String s3ObjectKey) throws IOException {
        // get local state if it exists
        final File persistenceFile = getPersistenceFile();

        if (persistenceFile.exists()) {
            final Properties props = new Properties();
            try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                props.load(fis);
            } catch (IOException ioe) {
                getLogger().warn("Failed to recover local state for {} due to {}. Assuming no local state and " +
                        "restarting upload.", new Object[]{s3ObjectKey, ioe.getMessage()});
                return null;
            }
            if (props.containsKey(s3ObjectKey)) {
                final String localSerialState = props.getProperty(s3ObjectKey);
                if (localSerialState != null) {
                    try {
                        return new MultipartState(localSerialState);
                    } catch (final RuntimeException rte) {
                        getLogger().warn("Failed to recover local state for {} due to corrupt data in state.", new Object[]{s3ObjectKey, rte.getMessage()});
                        return null;
                    }
                }
            }
        }
        return null;
    }

    protected synchronized void persistLocalState(final String s3ObjectKey, final MultipartState currState) throws IOException {
        final String currStateStr = (currState == null) ? null : currState.toString();
        final File persistenceFile = getPersistenceFile();
        final File parentDir = persistenceFile.getParentFile();
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new IOException("Persistence directory (" + parentDir.getAbsolutePath() + ") does not exist and " +
                    "could not be created.");
        }
        final Properties props = new Properties();
        if (persistenceFile.exists()) {
            try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                props.load(fis);
            }
        }
        if (currStateStr != null) {
            currState.setTimestamp(System.currentTimeMillis());
            props.setProperty(s3ObjectKey, currStateStr);
        } else {
            props.remove(s3ObjectKey);
        }

        if (props.size() > 0) {
            try (final FileOutputStream fos = new FileOutputStream(persistenceFile)) {
                props.store(fos, null);
            } catch (IOException ioe) {
                getLogger().error("Could not store state {} due to {}.",
                        new Object[]{persistenceFile.getAbsolutePath(), ioe.getMessage()});
            }
        } else {
            if (persistenceFile.exists()) {
                try {
                    Files.delete(persistenceFile.toPath());
                } catch (IOException ioe) {
                    getLogger().error("Could not remove state file {} due to {}.",
                            new Object[]{persistenceFile.getAbsolutePath(), ioe.getMessage()});
                }
            }
        }
    }

    protected synchronized void removeLocalState(final String s3ObjectKey) throws IOException {
        persistLocalState(s3ObjectKey, null);
    }

    private synchronized void ageoffLocalState(long ageCutoff) {
        // get local state if it exists
        final File persistenceFile = getPersistenceFile();
        if (persistenceFile.exists()) {
            Properties props = new Properties();
            try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                props.load(fis);
            } catch (final IOException ioe) {
                getLogger().warn("Failed to ageoff remove local state due to {}",
                        new Object[]{ioe.getMessage()});
                return;
            }
            for (Entry<Object,Object> entry: props.entrySet()) {
                final String key = (String) entry.getKey();
                final String localSerialState = props.getProperty(key);
                if (localSerialState != null) {
                    final MultipartState state = new MultipartState(localSerialState);
                    if (state.getTimestamp() < ageCutoff) {
                        getLogger().warn("Removing local state for {} due to exceeding ageoff time",
                                new Object[]{key});
                        try {
                            removeLocalState(key);
                        } catch (final IOException ioe) {
                            getLogger().warn("Failed to remove local state for {} due to {}",
                                    new Object[]{key, ioe.getMessage()});

                        }
                    }
                }
            }
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records){

        final long startNanos = System.nanoTime();

        try {
            for (Record record : records) {
                final String bucket = context.getPropertyValue(BUCKET_FIELD).evaluate(record).asString();
                final String key = context.getPropertyValue(KEY_FEILD).evaluate(record).asString();
                final String cacheKey = getIdentifier() + "/" + bucket + "/" + key;

                final AmazonS3Client s3 = getClient();

                final String ffFilename = record.getField("filename").getRawValue().toString();
                record.setField(new Field(S3_BUCKET_KEY, bucket));
                record.setField(new Field(S3_OBJECT_KEY, key));

                final Long multipartThreshold = context.getPropertyValue(MULTIPART_THRESHOLD).asDataSize(DataUnit.B).longValue();
                final Long multipartPartSize = context.getPropertyValue(MULTIPART_PART_SIZE).asDataSize(DataUnit.B).longValue();

                final long now = System.currentTimeMillis();

                /*
                 * If necessary, run age off for existing uploads in AWS S3 and local state
                 */
                ageoffS3Uploads(context, s3, now, bucket);

                /*
                 * Then
                 */
                try {

                    try (final InputStream in = new ByteArrayInputStream((byte[]) record.getField(FieldDictionary.RECORD_VALUE).getRawValue())) {
                        final ObjectMetadata objectMetadata = new ObjectMetadata();
                        objectMetadata.setContentDisposition(URLEncoder.encode(ffFilename, "UTF-8"));
                        objectMetadata.setContentLength(record.size()); // TODO shouldn't i use the size of record.getField(FieldDictionary.RECORD_VALUE).getRawValue()

                        final String contentType = context.getPropertyValue(CONTENT_TYPE)
                                .evaluate(record).asString();
                        if (contentType != null) {
                            objectMetadata.setContentType(contentType);
                            record.setField(new Field(S3_CONTENT_TYPE, contentType));
                        }

                        final String expirationRule = context.getPropertyValue(EXPIRATION_RULE_ID)
                                .evaluate(record).asString();
                        if (expirationRule != null) {
                            objectMetadata.setExpirationTimeRuleId(expirationRule);
                        }

                        final Map<String, String> userMetadata = new HashMap<>();
                        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                            if (entry.getKey().isDynamic()) {
                                final String value = context.getPropertyValue(
                                        entry.getKey()).evaluate(record).asString();
                                userMetadata.put(entry.getKey().getName(), value);
                            }
                        }

                        final String serverSideEncryption = context.getPropertyValue(SERVER_SIDE_ENCRYPTION).asString();
                        AmazonS3EncryptionService encryptionService = null;

                        if (!serverSideEncryption.equals(NO_SERVER_SIDE_ENCRYPTION)) {
                            objectMetadata.setSSEAlgorithm(serverSideEncryption);
                            record.setField(new Field(S3_SSE_ALGORITHM, serverSideEncryption));
                        } else {
                            encryptionService = context.getPropertyValue(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
                        }

                        if (!userMetadata.isEmpty()) {
                            objectMetadata.setUserMetadata(userMetadata);
                        }

                        if (record.size() <= multipartThreshold) { // TODO shouldn't i use the size of record.getField(FieldDictionary.RECORD_VALUE).getRawValue()
                            //----------------------------------------
                            // single part upload
                            //----------------------------------------
                            final PutObjectRequest request = new PutObjectRequest(bucket, key, in, objectMetadata);
                            if (encryptionService != null) {
                                encryptionService.configurePutObjectRequest(request, objectMetadata);
                                record.setField(new Field(S3_ENCRYPTION_STRATEGY, encryptionService.getStrategyName()));
                            }

                            request.setStorageClass(StorageClass.valueOf(context.getPropertyValue(STORAGE_CLASS).asString()));
                            final AccessControlList acl = createACL(context, record);
                            if (acl != null) {
                                request.setAccessControlList(acl);
                            }

                            /*final CannedAccessControlList cannedAcl = createCannedACL(context, record);
                            if (cannedAcl != null) {
                                request.withCannedAcl(cannedAcl);
                            }*/ // TODO see how to fix this

                            if (context.getPropertyValue(OBJECT_TAGS_PREFIX).isSet()) {
                                request.setTagging(new ObjectTagging(getObjectTags(context, record)));
                            }

                            try {
                                final PutObjectResult result = s3.putObject(request);
                                if (result.getVersionId() != null) {
                                    record.setField(new Field(S3_VERSION_ATTR_KEY, result.getVersionId()));
                                }
                                if (result.getETag() != null) {
                                    record.setField(new Field(S3_ETAG_ATTR_KEY, result.getETag()));
                                }
                                if (result.getExpirationTime() != null) {
                                    record.setField(new Field(S3_EXPIRATION_ATTR_KEY, result.getExpirationTime().toString()));
                                }
                                if (result.getMetadata().getStorageClass() != null) {
                                    record.setField(new Field(S3_STORAGECLASS_ATTR_KEY, result.getMetadata().getStorageClass()));
                                } else {
                                    record.setField(new Field(S3_STORAGECLASS_ATTR_KEY, StorageClass.Standard.toString()));
                                }
                                if (userMetadata.size() > 0) {
                                    StringBuilder userMetaBldr = new StringBuilder();
                                    for (String userKey : userMetadata.keySet()) {
                                        userMetaBldr.append(userKey).append("=").append(userMetadata.get(userKey));
                                    }
                                    record.setField(new Field(S3_USERMETA_ATTR_KEY, userMetaBldr.toString()));
                                }
                                record.setField(new Field(S3_API_METHOD_ATTR_KEY, S3_API_METHOD_PUTOBJECT));
                            } catch (AmazonClientException e) {
                                getLogger().info("Failure completing upload file={} bucket={} key={} reason={}",
                                        new Object[]{ffFilename, bucket, key, e.getMessage()});
                                throw (e);
                            }
                        } else {
                            //----------------------------------------
                            // multipart upload
                            //----------------------------------------

                            // load or create persistent state
                            //------------------------------------------------------------
                            MultipartState currentState;
                            try {
                                currentState = getLocalStateIfInS3(s3, bucket, cacheKey);
                                if (currentState != null) {
                                    if (currentState.getPartETags().size() > 0) {
                                        final PartETag lastETag = currentState.getPartETags().get(
                                                currentState.getPartETags().size() - 1);
                                        getLogger().info("Resuming upload for file='{}' bucket='{}' key='{}' " +
                                                        "uploadID='{}' filePosition='{}' partSize='{}' storageClass='{}' " +
                                                        "contentLength='{}' partsLoaded={} lastPart={}/{}",
                                                new Object[]{ffFilename, bucket, key, currentState.getUploadId(),
                                                        currentState.getFilePosition(), currentState.getPartSize(),
                                                        currentState.getStorageClass().toString(),
                                                        currentState.getContentLength(),
                                                        currentState.getPartETags().size(),
                                                        Integer.toString(lastETag.getPartNumber()),
                                                        lastETag.getETag()});
                                    } else {
                                        getLogger().info("Resuming upload for file='{}' bucket='{}' key='{}' " +
                                                        "uploadID='{}' filePosition='{}' partSize='{}' storageClass='{}' " +
                                                        "contentLength='{}' no partsLoaded",
                                                new Object[]{ffFilename, bucket, key, currentState.getUploadId(),
                                                        currentState.getFilePosition(), currentState.getPartSize(),
                                                        currentState.getStorageClass().toString(),
                                                        currentState.getContentLength()});
                                    }
                                } else {
                                    currentState = new MultipartState();
                                    currentState.setPartSize(multipartPartSize);
                                    currentState.setStorageClass(
                                            StorageClass.valueOf(context.getPropertyValue(STORAGE_CLASS).getRawValue().toString()));
                                    currentState.setContentLength((long) record.size());
                                    persistLocalState(cacheKey, currentState);
                                    getLogger().info("Starting new upload for file='{}' bucket='{}' key='{}'",
                                            new Object[]{ffFilename, bucket, key});
                                }
                            } catch (IOException e) {
                                getLogger().error("IOException initiating cache state while processing flow files: " +
                                        e.getMessage());
                                throw (e);
                            }

                            // initiate multipart upload or find position in file
                            //------------------------------------------------------------
                            if (currentState.getUploadId().isEmpty()) {
                                final InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucket, key, objectMetadata);
                                if (encryptionService != null) {
                                    encryptionService.configureInitiateMultipartUploadRequest(initiateRequest, objectMetadata);
                                    record.setField(new Field(S3_ENCRYPTION_STRATEGY, encryptionService.getStrategyName()));
                                }
                                initiateRequest.setStorageClass(currentState.getStorageClass());

                                final AccessControlList acl = createACL(context, record);
                                if (acl != null) {
                                    initiateRequest.setAccessControlList(acl);
                                }
                                final CannedAccessControlList cannedAcl = createCannedACL(context, record);
                                if (cannedAcl != null) {
                                    initiateRequest.withCannedACL(cannedAcl);
                                }

                                if (context.getPropertyValue(OBJECT_TAGS_PREFIX).isSet()) {
                                    initiateRequest.setTagging(new ObjectTagging(getObjectTags(context, record)));
                                }

                                try {
                                    final InitiateMultipartUploadResult initiateResult =
                                            s3.initiateMultipartUpload(initiateRequest);
                                    currentState.setUploadId(initiateResult.getUploadId());
                                    currentState.getPartETags().clear();
                                    try {
                                        persistLocalState(cacheKey, currentState);
                                    } catch (Exception e) {
                                        getLogger().info("Exception saving cache state while processing file: " +
                                                e.getMessage());
                                        throw(new ProcessException("Exception saving cache state" + e.getMessage()));
                                    }
                                    getLogger().info("Success initiating upload file={} available={} position={} " +
                                                    "length={} bucket={} key={} uploadId={}",
                                            new Object[]{ffFilename, in.available(), currentState.getFilePosition(),
                                                    currentState.getContentLength(), bucket, key,
                                                    currentState.getUploadId()});
                                    if (initiateResult.getUploadId() != null) {
                                        record.setField(new Field(S3_UPLOAD_ID_ATTR_KEY, initiateResult.getUploadId()));
                                    }
                                } catch (AmazonClientException e) {
                                    getLogger().info("Failure initiating upload file={} bucket={} key={} reason={}",
                                            new Object[]{ffFilename, bucket, key, e.getMessage()});
                                    throw(e);
                                }
                            } else {
                                if (currentState.getFilePosition() > 0) {
                                    try {
                                        final long skipped = in.skip(currentState.getFilePosition());
                                        if (skipped != currentState.getFilePosition()) {
                                            getLogger().info("Failure skipping to resume upload file={} " +
                                                            "bucket={} key={} position={} skipped={}",
                                                    new Object[]{ffFilename, bucket, key,
                                                            currentState.getFilePosition(), skipped});
                                        }
                                    } catch (Exception e) {
                                        getLogger().info("Failure skipping to resume upload file={} bucket={} " +
                                                        "key={} position={} reason={}",
                                                new Object[]{ffFilename, bucket, key, currentState.getFilePosition(),
                                                        e.getMessage()});
                                        throw(new ProcessException(e.getMessage()));
                                    }
                                }
                            }

                            // upload parts
                            //------------------------------------------------------------
                            long thisPartSize;
                            boolean isLastPart;
                            for (int part = currentState.getPartETags().size() + 1;
                                 currentState.getFilePosition() < currentState.getContentLength(); part++) {
                                /*if (!PutS3Object.this.isScheduled()) {
                                    throw new IOException(S3_PROCESS_UNSCHEDULED_MESSAGE + " flowfile=" + ffFilename +
                                            " part=" + part + " uploadId=" + currentState.getUploadId());
                                }*/
                                thisPartSize = Math.min(currentState.getPartSize(),
                                        (currentState.getContentLength() - currentState.getFilePosition()));
                                isLastPart = currentState.getContentLength() == currentState.getFilePosition() + thisPartSize;
                                UploadPartRequest uploadRequest = new UploadPartRequest()
                                        .withBucketName(bucket)
                                        .withKey(key)
                                        .withUploadId(currentState.getUploadId())
                                        .withInputStream(in)
                                        .withPartNumber(part)
                                        .withPartSize(thisPartSize)
                                        .withLastPart(isLastPart);
                                if (encryptionService != null) {
                                    encryptionService.configureUploadPartRequest(uploadRequest, objectMetadata);
                                }
                                try {
                                    UploadPartResult uploadPartResult = s3.uploadPart(uploadRequest);
                                    currentState.addPartETag(uploadPartResult.getPartETag());
                                    currentState.setFilePosition(currentState.getFilePosition() + thisPartSize);
                                    try {
                                        persistLocalState(cacheKey, currentState);
                                    } catch (Exception e) {
                                        getLogger().info("Exception saving cache state processing file: " +
                                                e.getMessage());
                                    }
                                    int available = 0;
                                    try {
                                        available = in.available();
                                    } catch (IOException e) {
                                        // in case of the last part, the stream is already closed
                                    }
                                    getLogger().info("Success uploading part file={} part={} available={} " +
                                            "etag={} uploadId={}", new Object[]{ffFilename, part, available,
                                            uploadPartResult.getETag(), currentState.getUploadId()});
                                } catch (AmazonClientException e) {
                                    getLogger().info("Failure uploading part file={} part={} bucket={} key={} " +
                                            "reason={}", new Object[]{ffFilename, part, bucket, key, e.getMessage()});
                                    throw (e);
                                }
                            }

                            // complete multipart upload
                            //------------------------------------------------------------
                            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                                    bucket, key, currentState.getUploadId(), currentState.getPartETags());

                            // No call to an encryption service is needed for a CompleteMultipartUploadRequest.
                            try {
                                CompleteMultipartUploadResult completeResult =
                                        s3.completeMultipartUpload(completeRequest);
                                getLogger().info("Success completing upload file={} etag={} uploadId={}",
                                        new Object[]{ffFilename, completeResult.getETag(), currentState.getUploadId()});
                                if (completeResult.getVersionId() != null) {
                                    record.setField(new Field(S3_VERSION_ATTR_KEY, completeResult.getVersionId()));
                                }
                                if (completeResult.getETag() != null) {
                                    record.setField(new Field(S3_ETAG_ATTR_KEY, completeResult.getETag()));
                                }
                                if (completeResult.getExpirationTime() != null) {
                                    record.setField(new Field(S3_EXPIRATION_ATTR_KEY,
                                            completeResult.getExpirationTime().toString()));
                                }
                                if (currentState.getStorageClass() != null) {
                                    record.setField(new Field(S3_STORAGECLASS_ATTR_KEY, currentState.getStorageClass().toString()));
                                }
                                if (userMetadata.size() > 0) {
                                    StringBuilder userMetaBldr = new StringBuilder();
                                    for (String userKey : userMetadata.keySet()) {
                                        userMetaBldr.append(userKey).append("=").append(userMetadata.get(userKey));
                                    }
                                    record.setField(new Field(S3_USERMETA_ATTR_KEY, userMetaBldr.toString()));
                                }
                                record.setField(new Field(S3_API_METHOD_ATTR_KEY, S3_API_METHOD_MULTIPARTUPLOAD));
                            } catch (AmazonClientException e) {
                                getLogger().info("Failure completing upload file={} bucket={} key={} reason={}",
                                        new Object[]{ffFilename, bucket, key, e.getMessage()});
                                throw (e);
                            }
                        }
                    }


                    final String url = s3.getResourceUrl(bucket, key);
                    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    /*session.getProvenanceReporter().send(flowFile, url, millis);*/
                    // TODO see how to replcae this

                    getLogger().info("Successfully put {} to Amazon S3 in {} milliseconds", new Object[] {record, millis});
                    try {
                        removeLocalState(cacheKey);
                    } catch (IOException e) {
                        getLogger().info("Error trying to delete key {} from cache: {}",
                                new Object[]{cacheKey, e.getMessage()});
                    }
                } catch (final ProcessException | AmazonClientException pe) {
                    if (pe.getMessage().contains(S3_PROCESS_UNSCHEDULED_MESSAGE)) {
                        getLogger().info(pe.getMessage());
                        /*session.rollback();*/
                        // TODO see how to replcae this
                    } else {
                        getLogger().error("Failed to put {} to Amazon S3 due to {}", new Object[]{record, pe});
                        /*flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);*/
                        // TODO see how to replcae this
                        record.addError("Failed to put {} to Amazon S3 due to {}", getLogger(),"Failed to put {} to Amazon S3 due to {}", new Object[]{pe});
                    }
                }

            }
        }catch (Throwable t) {
            getLogger().error("error while processing records ", t);
        }
        return records;
    }

    private final Lock s3BucketLock = new ReentrantLock();
    private final AtomicLong lastS3AgeOff = new AtomicLong(0L);
    private final DateFormat logFormat = new SimpleDateFormat();

    protected void ageoffS3Uploads(final ProcessContext context, final AmazonS3Client s3, final long now, String bucket) {
        MultipartUploadListing oldUploads = getS3AgeoffListAndAgeoffLocalState(context, s3, now, bucket);
        for (MultipartUpload upload : oldUploads.getMultipartUploads()) {
            abortS3MultipartUpload(s3, oldUploads.getBucketName(), upload);
        }
    }

    protected MultipartUploadListing getS3AgeoffListAndAgeoffLocalState(final ProcessContext context, final AmazonS3Client s3, final long now, String bucket) {
        final long ageoff_interval = context.getPropertyValue(MULTIPART_S3_AGEOFF_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getPropertyValue(MULTIPART_S3_MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long ageCutoff = now - maxAge;

        final List<MultipartUpload> ageoffList = new ArrayList<>();
        if ((lastS3AgeOff.get() < now - ageoff_interval) && s3BucketLock.tryLock()) {
            try {

                ListMultipartUploadsRequest listRequest = new ListMultipartUploadsRequest(bucket);
                MultipartUploadListing listing = s3.listMultipartUploads(listRequest);
                for (MultipartUpload upload : listing.getMultipartUploads()) {
                    long uploadTime = upload.getInitiated().getTime();
                    if (uploadTime < ageCutoff) {
                        ageoffList.add(upload);
                    }
                }

                // ageoff any local state
                ageoffLocalState(ageCutoff);
                lastS3AgeOff.set(System.currentTimeMillis());
            } catch(AmazonClientException e) {
                if (e instanceof AmazonS3Exception
                        && ((AmazonS3Exception)e).getStatusCode() == 403
                        && ((AmazonS3Exception) e).getErrorCode().equals("AccessDenied")) {
                    getLogger().warn("AccessDenied checking S3 Multipart Upload list for {}: {} " +
                                    "** The configured user does not have the s3:ListBucketMultipartUploads permission " +
                                    "for this bucket, S3 ageoff cannot occur without this permission.  Next ageoff check " +
                                    "time is being advanced by interval to prevent checking on every upload **",
                            new Object[]{bucket, e.getMessage()});
                    lastS3AgeOff.set(System.currentTimeMillis());
                } else {
                    getLogger().error("Error checking S3 Multipart Upload list for {}: {}",
                            new Object[]{bucket, e.getMessage()});
                }
            } finally {
                s3BucketLock.unlock();
            }
        }
        MultipartUploadListing result = new MultipartUploadListing();
        result.setBucketName(bucket);
        result.setMultipartUploads(ageoffList);
        return result;
    }

    protected void abortS3MultipartUpload(final AmazonS3Client s3, final String bucket, final MultipartUpload upload) {
        final String uploadKey = upload.getKey();
        final String uploadId = upload.getUploadId();
        final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(
                bucket, uploadKey, uploadId);
        // No call to an encryption service is necessary for an AbortMultipartUploadRequest.
        try {
            s3.abortMultipartUpload(abortRequest);
            getLogger().info("Aborting out of date multipart upload, bucket {} key {} ID {}, initiated {}",
                    new Object[]{bucket, uploadKey, uploadId, logFormat.format(upload.getInitiated())});
        } catch (AmazonClientException ace) {
            getLogger().info("Error trying to abort multipart upload from bucket {} with key {} and ID {}: {}",
                    new Object[]{bucket, uploadKey, uploadId, ace.getMessage()});
        }
    }

    private List<Tag> getObjectTags(ProcessContext context, Record record) {
        final String prefix = context.getPropertyValue(OBJECT_TAGS_PREFIX).evaluate(record).asString();
        final List<Tag> objectTags = new ArrayList<>();

        record.getFieldsEntrySet().stream().sequential()
                .filter(attribute -> attribute.getKey().startsWith(prefix))
                .forEach(attribute -> {
                    String tagKey = attribute.getKey();
                    Object tagValue = attribute.getValue().getRawValue();

                    if (context.getPropertyValue(REMOVE_TAG_PREFIX).asBoolean()) {
                        tagKey = tagKey.replace(prefix, "");
                    }
                    objectTags.add(new Tag(tagKey, tagValue.toString()));
                });

        return objectTags;
    }

    protected static class MultipartState implements Serializable {

        private static final long serialVersionUID = 9006072180563519740L;

        private static final String SEPARATOR = "#";

        private String _uploadId;
        private Long _filePosition;
        private List<PartETag> _partETags;
        private Long _partSize;
        private StorageClass _storageClass;
        private Long _contentLength;
        private Long _timestamp;

        public MultipartState() {
            _uploadId = "";
            _filePosition = 0L;
            _partETags = new ArrayList<>();
            _partSize = 0L;
            _storageClass = StorageClass.Standard;
            _contentLength = 0L;
            _timestamp = System.currentTimeMillis();
        }

        // create from a previous toString() result
        public MultipartState(String buf) {
            String[] fields = buf.split(SEPARATOR);
            _uploadId = fields[0];
            _filePosition = Long.parseLong(fields[1]);
            _partETags = new ArrayList<>();
            for (String part : fields[2].split(",")) {
                if (part != null && !part.isEmpty()) {
                    String[] partFields = part.split("/");
                    _partETags.add(new PartETag(Integer.parseInt(partFields[0]), partFields[1]));
                }
            }
            _partSize = Long.parseLong(fields[3]);
            _storageClass = StorageClass.fromValue(fields[4]);
            _contentLength = Long.parseLong(fields[5]);
            _timestamp = Long.parseLong(fields[6]);
        }

        public String getUploadId() {
            return _uploadId;
        }

        public void setUploadId(String id) {
            _uploadId = id;
        }

        public Long getFilePosition() {
            return _filePosition;
        }

        public void setFilePosition(Long pos) {
            _filePosition = pos;
        }

        public List<PartETag> getPartETags() {
            return _partETags;
        }

        public void addPartETag(PartETag tag) {
            _partETags.add(tag);
        }

        public Long getPartSize() {
            return _partSize;
        }

        public void setPartSize(Long size) {
            _partSize = size;
        }

        public StorageClass getStorageClass() {
            return _storageClass;
        }

        public void setStorageClass(StorageClass aClass) {
            _storageClass = aClass;
        }

        public Long getContentLength() {
            return _contentLength;
        }

        public void setContentLength(Long length) {
            _contentLength = length;
        }

        public Long getTimestamp() {
            return _timestamp;
        }

        public void setTimestamp(Long timestamp) {
            _timestamp = timestamp;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append(_uploadId).append(SEPARATOR)
                    .append(_filePosition.toString()).append(SEPARATOR);
            if (_partETags.size() > 0) {
                boolean first = true;
                for (PartETag tag : _partETags) {
                    if (!first) {
                        buf.append(",");
                    } else {
                        first = false;
                    }
                    buf.append(String.format("%d/%s", tag.getPartNumber(), tag.getETag()));
                }
            }
            buf.append(SEPARATOR)
                    .append(_partSize.toString()).append(SEPARATOR)
                    .append(_storageClass.toString()).append(SEPARATOR)
                    .append(_contentLength.toString()).append(SEPARATOR)
                    .append(_timestamp.toString());
            return buf.toString();
        }
    }
}