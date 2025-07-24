/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.CharBuffer;

import org.opensearch.SpecialPermission;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.shared.Constants;
import org.opensearch.secure_sm.AccessController;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Ip2Geo datasource manifest file object
 *
 * Manifest file is stored in an external endpoint. OpenSearch read the file and store values it in this object.
 */
@Setter
@Getter
@AllArgsConstructor
public class DatasourceManifest {
    private static final ParseField URL_FIELD = new ParseField("url");
    private static final ParseField DB_NAME_FIELD = new ParseField("db_name");
    private static final ParseField SHA256_HASH_FIELD = new ParseField("sha256_hash");
    private static final ParseField VALID_FOR_IN_DAYS_FIELD = new ParseField("valid_for_in_days");
    private static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at_in_epoch_milli");
    private static final ParseField PROVIDER_FIELD = new ParseField("provider");

    /**
     * @param url URL of a ZIP file containing a database
     * @return URL of a ZIP file containing a database
     */
    private String url;
    /**
     * @param dbName A database file name inside the ZIP file
     * @return A database file name inside the ZIP file
     */
    private String dbName;
    /**
     * @param sha256Hash SHA256 hash value of a database file
     * @return SHA256 hash value of a database file
     */
    private String sha256Hash;
    /**
     * @param validForInDays A duration in which the database file is valid to use
     * @return A duration in which the database file is valid to use
     */
    private Long validForInDays;
    /**
     * @param updatedAt A date when the database was updated
     * @return A date when the database was updated
     */
    private Long updatedAt;
    /**
     * @param provider A database provider name
     * @return A database provider name
     */
    private String provider;

    /**
     * Ddatasource manifest parser
     */
    public static final ConstructingObjectParser<DatasourceManifest, Void> PARSER = new ConstructingObjectParser<>(
        "datasource_manifest",
        true,
        args -> {
            String url = (String) args[0];
            String dbName = (String) args[1];
            String sha256Hash = (String) args[2];
            Long validForInDays = (Long) args[3];
            Long updatedAt = (Long) args[4];
            String provider = (String) args[5];
            return new DatasourceManifest(url, dbName, sha256Hash, validForInDays, updatedAt, provider);
        }
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), URL_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DB_NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SHA256_HASH_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VALID_FOR_IN_DAYS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), UPDATED_AT_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PROVIDER_FIELD);
    }

    /**
     * Datasource manifest builder
     */
    public static class Builder {
        private static final int MANIFEST_FILE_MAX_BYTES = 1024 * 8;

        /**
         * Build DatasourceManifest from a given url
         *
         * @param url url to downloads a manifest file
         * @return DatasourceManifest representing the manifest file
         */
        @SuppressForbidden(reason = "Need to connect to http endpoint to read manifest file")
        public static DatasourceManifest build(final URL url) {
            SpecialPermission.check();
            return AccessController.doPrivileged(() -> {
                try {
                    URLConnection connection = url.openConnection();
                    return internalBuild(connection);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @VisibleForTesting
        @SuppressForbidden(reason = "Need to connect to http endpoint to read manifest file")
        protected static DatasourceManifest internalBuild(final URLConnection connection) throws IOException {
            connection.addRequestProperty(Constants.USER_AGENT_KEY, Constants.USER_AGENT_VALUE);
            if (connection instanceof HttpURLConnection) {
                HttpRedirectValidator.validateNoRedirects((HttpURLConnection) connection);
            }
            InputStreamReader inputStreamReader = new InputStreamReader(connection.getInputStream());
            try (BufferedReader reader = new BufferedReader(inputStreamReader)) {
                CharBuffer charBuffer = CharBuffer.allocate(MANIFEST_FILE_MAX_BYTES);
                reader.read(charBuffer);
                charBuffer.flip();
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.IGNORE_DEPRECATIONS,
                    charBuffer.toString()
                );
                return PARSER.parse(parser, null);
            }
        }
    }
}
