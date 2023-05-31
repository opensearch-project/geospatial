/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.action.PutDatasourceRequest;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;

/**
 * Ip2Geo datasource job parameter
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class Datasource implements Writeable, ScheduledJobParameter {
    /**
     * Prefix of indices having Ip2Geo data
     */
    public static final String IP2GEO_DATA_INDEX_NAME_PREFIX = ".ip2geo-data";

    /**
     * Default fields for job scheduling
     */
    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField ENABLED_FIELD = new ParseField("update_enabled");
    private static final ParseField LAST_UPDATE_TIME_FIELD = new ParseField("last_update_time");
    private static final ParseField LAST_UPDATE_TIME_FIELD_READABLE = new ParseField("last_update_time_field");
    /**
     * Schedule that user set
     */
    private static final ParseField USER_SCHEDULE_FIELD = new ParseField("user_schedule");
    /**
     * System schedule which will be used by job scheduler
     *
     * If datasource is going to get expired before next update, we want to run clean up task before the next update
     * by changing system schedule.
     *
     * If datasource is restored from snapshot, we want to run clean up task immediately to handle expired datasource
     * by changing system schedule.
     *
     * For every task run, we revert system schedule back to user schedule.
     */
    private static final ParseField SYSTEM_SCHEDULE_FIELD = new ParseField("system_schedule");
    /**
     * {@link DatasourceTask} that DatasourceRunner will execute in next run
     *
     * For every task run, we revert task back to {@link DatasourceTask#ALL}
     */
    private static final ParseField TASK_FIELD = new ParseField("task");
    private static final ParseField ENABLED_TIME_FIELD = new ParseField("enabled_time");
    private static final ParseField ENABLED_TIME_FIELD_READABLE = new ParseField("enabled_time_field");

    /**
     * Additional fields for datasource
     */
    private static final ParseField ENDPOINT_FIELD = new ParseField("endpoint");
    private static final ParseField STATE_FIELD = new ParseField("state");
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField DATABASE_FIELD = new ParseField("database");
    private static final ParseField UPDATE_STATS_FIELD = new ParseField("update_stats");

    /**
     * Default variables for job scheduling
     */

    /**
     * @param name name of a datasource
     * @return name of a datasource
     */
    private String name;
    /**
     * @param lastUpdateTime Last update time of a datasource
     * @return Last update time of a datasource
     */
    private Instant lastUpdateTime;
    /**
     * @param enabledTime Last time when a scheduling is enabled for a GeoIP data update
     * @return Last time when a scheduling is enabled for the job scheduler
     */
    private Instant enabledTime;
    /**
     * @param isEnabled Indicate if GeoIP data update is scheduled or not
     * @return Indicate if scheduling is enabled or not
     */
    private boolean isEnabled;
    /**
     * @param userSchedule Schedule that user provided
     * @return Schedule that user provided
     */
    private IntervalSchedule userSchedule;

    /**
     * @param systemSchedule Schedule that job scheduler use
     * @return Schedule that job scheduler use
     */
    private IntervalSchedule systemSchedule;

    /**
     * @param task Task that {@link DatasourceRunner} will execute
     * @return Task that {@link DatasourceRunner} will execute
     */
    private DatasourceTask task;

    /**
     * Additional variables for datasource
     */

    /**
     * @param endpoint URL of a manifest file
     * @return URL of a manifest file
     */
    private String endpoint;
    /**
     * @param state State of a datasource
     * @return State of a datasource
     */
    private DatasourceState state;
    /**
     * @param indices A list of indices having GeoIP data
     * @return A list of indices having GeoIP data
     */
    private List<String> indices;
    /**
     * @param database GeoIP database information
     * @return GeoIP database information
     */
    private Database database;
    /**
     * @param updateStats GeoIP database update statistics
     * @return GeoIP database update statistics
     */
    private UpdateStats updateStats;

    /**
     * Datasource parser
     */
    public static final ConstructingObjectParser<Datasource, Void> PARSER = new ConstructingObjectParser<>(
        "datasource_metadata",
        true,
        args -> {
            String name = (String) args[0];
            Instant lastUpdateTime = Instant.ofEpochMilli((long) args[1]);
            Instant enabledTime = args[2] == null ? null : Instant.ofEpochMilli((long) args[2]);
            boolean isEnabled = (boolean) args[3];
            IntervalSchedule userSchedule = (IntervalSchedule) args[4];
            IntervalSchedule systemSchedule = (IntervalSchedule) args[5];
            DatasourceTask task = DatasourceTask.valueOf((String) args[6]);
            String endpoint = (String) args[7];
            DatasourceState state = DatasourceState.valueOf((String) args[8]);
            List<String> indices = (List<String>) args[9];
            Database database = (Database) args[10];
            UpdateStats updateStats = (UpdateStats) args[11];
            Datasource parameter = new Datasource(
                name,
                lastUpdateTime,
                enabledTime,
                isEnabled,
                userSchedule,
                systemSchedule,
                task,
                endpoint,
                state,
                indices,
                database,
                updateStats
            );

            return parameter;
        }
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_UPDATE_TIME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), ENABLED_TIME_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ScheduleParser.parse(p), USER_SCHEDULE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ScheduleParser.parse(p), SYSTEM_SCHEDULE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TASK_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ENDPOINT_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), STATE_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Database.PARSER, DATABASE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), UpdateStats.PARSER, UPDATE_STATS_FIELD);
    }

    @VisibleForTesting
    public Datasource() {
        this(null, null, null);
    }

    public Datasource(final String name, final IntervalSchedule schedule, final String endpoint) {
        this(
            name,
            Instant.now().truncatedTo(ChronoUnit.MILLIS),
            null,
            false,
            schedule,
            schedule,
            DatasourceTask.ALL,
            endpoint,
            DatasourceState.CREATING,
            new ArrayList<>(),
            new Database(),
            new UpdateStats()
        );
    }

    public Datasource(final StreamInput in) throws IOException {
        name = in.readString();
        lastUpdateTime = toInstant(in.readVLong());
        enabledTime = toInstant(in.readOptionalVLong());
        isEnabled = in.readBoolean();
        userSchedule = new IntervalSchedule(in);
        systemSchedule = new IntervalSchedule(in);
        task = DatasourceTask.valueOf(in.readString());
        endpoint = in.readString();
        state = DatasourceState.valueOf(in.readString());
        indices = in.readStringList();
        database = new Database(in);
        updateStats = new UpdateStats(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(lastUpdateTime.toEpochMilli());
        out.writeOptionalVLong(enabledTime == null ? null : enabledTime.toEpochMilli());
        out.writeBoolean(isEnabled);
        userSchedule.writeTo(out);
        systemSchedule.writeTo(out);
        out.writeString(task.name());
        out.writeString(endpoint);
        out.writeString(state.name());
        out.writeStringCollection(indices);
        database.writeTo(out);
        updateStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.timeField(
            LAST_UPDATE_TIME_FIELD.getPreferredName(),
            LAST_UPDATE_TIME_FIELD_READABLE.getPreferredName(),
            lastUpdateTime.toEpochMilli()
        );
        if (enabledTime != null) {
            builder.timeField(
                ENABLED_TIME_FIELD.getPreferredName(),
                ENABLED_TIME_FIELD_READABLE.getPreferredName(),
                enabledTime.toEpochMilli()
            );
        }
        builder.field(ENABLED_FIELD.getPreferredName(), isEnabled);
        builder.field(USER_SCHEDULE_FIELD.getPreferredName(), userSchedule);
        builder.field(SYSTEM_SCHEDULE_FIELD.getPreferredName(), systemSchedule);
        builder.field(TASK_FIELD.getPreferredName(), task.name());
        builder.field(ENDPOINT_FIELD.getPreferredName(), endpoint);
        builder.field(STATE_FIELD.getPreferredName(), state.name());
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(DATABASE_FIELD.getPreferredName(), database);
        builder.field(UPDATE_STATS_FIELD.getPreferredName(), updateStats);
        builder.endObject();
        return builder;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public Instant getEnabledTime() {
        return enabledTime;
    }

    @Override
    public IntervalSchedule getSchedule() {
        return systemSchedule;
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public Long getLockDurationSeconds() {
        return Ip2GeoLockService.LOCK_DURATION_IN_SECONDS;
    }

    /**
     * Enable auto update of GeoIP data
     */
    public void enable() {
        if (isEnabled == true) {
            return;
        }
        enabledTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        isEnabled = true;
    }

    /**
     * Disable auto update of GeoIP data
     */
    public void disable() {
        enabledTime = null;
        isEnabled = false;
    }

    /**
     * Current index name of a datasource
     *
     * @return Current index name of a datasource
     */
    public String currentIndexName() {
        return isExpired() ? null : indexNameFor(database.updatedAt.toEpochMilli());
    }

    /**
     * Index name for a given manifest
     *
     * @param manifest manifest
     * @return Index name for a given manifest
     */
    public String indexNameFor(final DatasourceManifest manifest) {
        return indexNameFor(manifest.getUpdatedAt());
    }

    private String indexNameFor(final long suffix) {
        return String.format(Locale.ROOT, "%s.%s.%d", IP2GEO_DATA_INDEX_NAME_PREFIX, name, suffix);
    }

    /**
     * Checks if datasource is expired or not
     *
     * @return true if datasource is expired, and false otherwise
     */
    public boolean isExpired() {
        return willExpire(Instant.now());
    }

    /**
     * Checks if datasource will expire at given time
     *
     * @return true if datasource will expired at given time, and false otherwise
     */
    public boolean willExpire(Instant instant) {
        if (database.validForInDays == null) {
            return false;
        }

        return instant.isAfter(expirationDay());
    }

    /**
     * Day when datasource will expire
     *
     * @return Day when datasource will expire
     */
    public Instant expirationDay() {
        if (database.validForInDays == null) {
            return Instant.MAX;
        }
        return lastCheckedAt().plus(database.validForInDays, ChronoUnit.DAYS);
    }

    private Instant lastCheckedAt() {
        Instant lastCheckedAt;
        if (updateStats.lastSkippedAt == null) {
            lastCheckedAt = updateStats.lastSucceededAt;
        } else {
            lastCheckedAt = updateStats.lastSucceededAt.isBefore(updateStats.lastSkippedAt)
                ? updateStats.lastSkippedAt
                : updateStats.lastSucceededAt;
        }
        return lastCheckedAt;
    }

    /**
     * Set database attributes with given input
     *
     * @param datasourceManifest the datasource manifest
     * @param fields the fields
     */
    public void setDatabase(final DatasourceManifest datasourceManifest, final List<String> fields) {
        this.database.setProvider(datasourceManifest.getProvider());
        this.database.setSha256Hash(datasourceManifest.getSha256Hash());
        this.database.setUpdatedAt(Instant.ofEpochMilli(datasourceManifest.getUpdatedAt()));
        this.database.setValidForInDays(datasourceManifest.getValidForInDays());
        this.database.setFields(fields);
    }

    /**
     * Checks if the database fields are compatible with the given set of fields.
     *
     * If database fields are null, it is compatible with any input fields
     * as it hasn't been generated before.
     *
     * @param fields The set of input fields to check for compatibility.
     * @return true if the database fields are compatible with the given input fields, false otherwise.
     */
    public boolean isCompatible(final List<String> fields) {
        if (database.fields == null) {
            return true;
        }

        if (fields.size() < database.fields.size()) {
            return false;
        }

        Set<String> fieldsSet = new HashSet<>(fields);
        for (String field : database.fields) {
            if (fieldsSet.contains(field) == false) {
                return false;
            }
        }
        return true;
    }

    private static Instant toInstant(final Long epochMilli) {
        return epochMilli == null ? null : Instant.ofEpochMilli(epochMilli);
    }

    /**
     * Database of a datasource
     */
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Database implements Writeable, ToXContent {
        private static final ParseField PROVIDER_FIELD = new ParseField("provider");
        private static final ParseField SHA256_HASH_FIELD = new ParseField("sha256_hash");
        private static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at_in_epoch_millis");
        private static final ParseField UPDATED_AT_FIELD_READABLE = new ParseField("updated_at");
        private static final ParseField FIELDS_FIELD = new ParseField("fields");
        private static final ParseField VALID_FOR_IN_DAYS_FIELD = new ParseField("valid_for_in_days");

        /**
         * @param provider A database provider name
         * @return A database provider name
         */
        private String provider;
        /**
         * @param sha256Hash SHA256 hash value of a database file
         * @return SHA256 hash value of a database file
         */
        private String sha256Hash;
        /**
         * @param updatedAt A date when the database was updated
         * @return A date when the database was updated
         */
        private Instant updatedAt;
        /**
         * @param validForInDays A duration in which the database file is valid to use
         * @return A duration in which the database file is valid to use
         */
        private Long validForInDays;
        /**
         * @param fields A list of available fields in the database
         * @return A list of available fields in the database
         */
        private List<String> fields;

        private static final ConstructingObjectParser<Database, Void> PARSER = new ConstructingObjectParser<>(
            "datasource_metadata_database",
            true,
            args -> {
                String provider = (String) args[0];
                String sha256Hash = (String) args[1];
                Instant updatedAt = args[2] == null ? null : Instant.ofEpochMilli((Long) args[2]);
                Long validForInDays = (Long) args[3];
                List<String> fields = (List<String>) args[4];
                return new Database(provider, sha256Hash, updatedAt, validForInDays, fields);
            }
        );
        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PROVIDER_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SHA256_HASH_FIELD);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), UPDATED_AT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VALID_FOR_IN_DAYS_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), FIELDS_FIELD);
        }

        public Database(final StreamInput in) throws IOException {
            provider = in.readOptionalString();
            sha256Hash = in.readOptionalString();
            updatedAt = toInstant(in.readOptionalVLong());
            validForInDays = in.readOptionalVLong();
            fields = in.readOptionalStringList();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeOptionalString(provider);
            out.writeOptionalString(sha256Hash);
            out.writeOptionalVLong(updatedAt.toEpochMilli());
            out.writeOptionalVLong(validForInDays);
            out.writeOptionalStringCollection(fields);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            if (provider != null) {
                builder.field(PROVIDER_FIELD.getPreferredName(), provider);
            }
            if (sha256Hash != null) {
                builder.field(SHA256_HASH_FIELD.getPreferredName(), sha256Hash);
            }
            if (updatedAt != null) {
                builder.timeField(
                    UPDATED_AT_FIELD.getPreferredName(),
                    UPDATED_AT_FIELD_READABLE.getPreferredName(),
                    updatedAt.toEpochMilli()
                );
            }
            if (validForInDays != null) {
                builder.field(VALID_FOR_IN_DAYS_FIELD.getPreferredName(), validForInDays);
            }
            if (fields != null) {
                builder.startArray(FIELDS_FIELD.getPreferredName());
                for (String field : fields) {
                    builder.value(field);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }
    }

    /**
     * Update stats of a datasource
     */
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class UpdateStats implements Writeable, ToXContent {
        private static final ParseField LAST_SUCCEEDED_AT_FIELD = new ParseField("last_succeeded_at_in_epoch_millis");
        private static final ParseField LAST_SUCCEEDED_AT_FIELD_READABLE = new ParseField("last_succeeded_at");
        private static final ParseField LAST_PROCESSING_TIME_IN_MILLIS_FIELD = new ParseField("last_processing_time_in_millis");
        private static final ParseField LAST_FAILED_AT_FIELD = new ParseField("last_failed_at_in_epoch_millis");
        private static final ParseField LAST_FAILED_AT_FIELD_READABLE = new ParseField("last_failed_at");
        private static final ParseField LAST_SKIPPED_AT = new ParseField("last_skipped_at_in_epoch_millis");
        private static final ParseField LAST_SKIPPED_AT_READABLE = new ParseField("last_skipped_at");

        /**
         * @param lastSucceededAt The last time when GeoIP data update was succeeded
         * @return The last time when GeoIP data update was succeeded
         */
        private Instant lastSucceededAt;
        /**
         * @param lastProcessingTimeInMillis The last processing time when GeoIP data update was succeeded
         * @return The last processing time when GeoIP data update was succeeded
         */
        private Long lastProcessingTimeInMillis;
        /**
         * @param lastFailedAt The last time when GeoIP data update was failed
         * @return The last time when GeoIP data update was failed
         */
        private Instant lastFailedAt;
        /**
         * @param lastSkippedAt The last time when GeoIP data update was skipped as there was no new update from an endpoint
         * @return The last time when GeoIP data update was skipped as there was no new update from an endpoint
         */
        private Instant lastSkippedAt;

        private static final ConstructingObjectParser<UpdateStats, Void> PARSER = new ConstructingObjectParser<>(
            "datasource_metadata_update_stats",
            true,
            args -> {
                Instant lastSucceededAt = args[0] == null ? null : Instant.ofEpochMilli((long) args[0]);
                Long lastProcessingTimeInMillis = (Long) args[1];
                Instant lastFailedAt = args[2] == null ? null : Instant.ofEpochMilli((long) args[2]);
                Instant lastSkippedAt = args[3] == null ? null : Instant.ofEpochMilli((long) args[3]);
                return new UpdateStats(lastSucceededAt, lastProcessingTimeInMillis, lastFailedAt, lastSkippedAt);
            }
        );

        static {
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LAST_SUCCEEDED_AT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LAST_PROCESSING_TIME_IN_MILLIS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LAST_FAILED_AT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LAST_SKIPPED_AT);
        }

        public UpdateStats(final StreamInput in) throws IOException {
            lastSucceededAt = toInstant(in.readOptionalVLong());
            lastProcessingTimeInMillis = in.readOptionalVLong();
            lastFailedAt = toInstant(in.readOptionalVLong());
            lastSkippedAt = toInstant(in.readOptionalVLong());
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeOptionalVLong(lastSucceededAt.toEpochMilli());
            out.writeOptionalVLong(lastProcessingTimeInMillis);
            out.writeOptionalVLong(lastFailedAt.toEpochMilli());
            out.writeOptionalVLong(lastSkippedAt.toEpochMilli());
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            if (lastSucceededAt != null) {
                builder.timeField(
                    LAST_SUCCEEDED_AT_FIELD.getPreferredName(),
                    LAST_SUCCEEDED_AT_FIELD_READABLE.getPreferredName(),
                    lastSucceededAt.toEpochMilli()
                );
            }
            if (lastProcessingTimeInMillis != null) {
                builder.field(LAST_PROCESSING_TIME_IN_MILLIS_FIELD.getPreferredName(), lastProcessingTimeInMillis);
            }
            if (lastFailedAt != null) {
                builder.timeField(
                    LAST_FAILED_AT_FIELD.getPreferredName(),
                    LAST_FAILED_AT_FIELD_READABLE.getPreferredName(),
                    lastFailedAt.toEpochMilli()
                );
            }
            if (lastSkippedAt != null) {
                builder.timeField(
                    LAST_SKIPPED_AT.getPreferredName(),
                    LAST_SKIPPED_AT_READABLE.getPreferredName(),
                    lastSkippedAt.toEpochMilli()
                );
            }
            builder.endObject();
            return builder;
        }
    }

    /**
     * Builder class for Datasource
     */
    public static class Builder {
        public static Datasource build(final PutDatasourceRequest request) {
            String id = request.getName();
            IntervalSchedule schedule = new IntervalSchedule(
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                (int) request.getUpdateInterval().days(),
                ChronoUnit.DAYS
            );
            String endpoint = request.getEndpoint();
            return new Datasource(id, schedule, endpoint);
        }
    }
}
