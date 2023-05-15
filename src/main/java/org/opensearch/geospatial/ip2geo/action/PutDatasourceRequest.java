/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Locale;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;

/**
 * Ip2Geo datasource creation request
 */
@Getter
@Setter
@Log4j2
@EqualsAndHashCode
public class PutDatasourceRequest extends AcknowledgedRequest<PutDatasourceRequest> {
    private static final int MAX_DATASOURCE_NAME_BYTES = 255;
    public static final ParseField ENDPOINT_FIELD = new ParseField("endpoint");
    public static final ParseField UPDATE_INTERVAL_IN_DAYS_FIELD = new ParseField("update_interval_in_days");
    /**
     * @param name the datasource name
     * @return the datasource name
     */
    private String name;
    /**
     * @param endpoint url to a manifest file for a datasource
     * @return url to a manifest file for a datasource
     */
    private String endpoint;
    /**
     * @param updateInterval update interval of a datasource
     * @return update interval of a datasource
     */
    private TimeValue updateInterval;

    /**
     * Parser of a datasource
     */
    public static final ObjectParser<PutDatasourceRequest, Void> PARSER;
    static {
        PARSER = new ObjectParser<>("put_datasource");
        PARSER.declareString((request, val) -> request.setEndpoint(val), ENDPOINT_FIELD);
        PARSER.declareLong((request, val) -> request.setUpdateInterval(TimeValue.timeValueDays(val)), UPDATE_INTERVAL_IN_DAYS_FIELD);
    }

    /**
     * Default constructor
     * @param name name of a datasource
     */
    public PutDatasourceRequest(final String name) {
        this.name = name;
    }

    /**
     * Constructor with stream input
     * @param in the stream input
     * @throws IOException IOException
     */
    public PutDatasourceRequest(final StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        this.endpoint = in.readString();
        this.updateInterval = in.readTimeValue();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(endpoint);
        out.writeTimeValue(updateInterval);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException errors = new ActionRequestValidationException();
        validateDatasourceName(errors);
        validateEndpoint(errors);
        validateUpdateInterval(errors);
        return errors.validationErrors().isEmpty() ? null : errors;
    }

    private void validateDatasourceName(final ActionRequestValidationException errors) {
        if (!Strings.validFileName(name)) {
            errors.addValidationError("Datasource name must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
            return;
        }
        if (name.isEmpty()) {
            errors.addValidationError("Datasource name must not be empty");
            return;
        }
        if (name.contains("#")) {
            errors.addValidationError("Datasource name must not contain '#'");
            return;
        }
        if (name.contains(":")) {
            errors.addValidationError("Datasource name must not contain ':'");
            return;
        }
        if (name.charAt(0) == '_' || name.charAt(0) == '-' || name.charAt(0) == '+') {
            errors.addValidationError("Datasource name must not start with '_', '-', or '+'");
            return;
        }
        int byteCount = 0;
        try {
            byteCount = name.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new OpenSearchException("Unable to determine length of datasource name", e);
        }
        if (byteCount > MAX_DATASOURCE_NAME_BYTES) {
            errors.addValidationError("Datasource name is too long, (" + byteCount + " > " + MAX_DATASOURCE_NAME_BYTES + ")");
            return;
        }
        if (name.equals(".") || name.equals("..")) {
            errors.addValidationError("Datasource name must not be '.' or '..'");
            return;
        }
    }

    /**
     * Conduct following validation on endpoint
     * 1. endpoint format complies with RFC-2396
     * 2. validate manifest file from the endpoint
     *
     * @param errors the errors to add error messages
     */
    private void validateEndpoint(final ActionRequestValidationException errors) {
        try {
            URL url = new URL(endpoint);
            url.toURI(); // Validate URL complies with RFC-2396
            validateManifestFile(url, errors);
        } catch (MalformedURLException | URISyntaxException e) {
            log.info("Invalid URL[{}] is provided", endpoint, e);
            errors.addValidationError("Invalid URL format is provided");
        }
    }

    /**
     * Conduct following validation on url
     * 1. can read manifest file from the endpoint
     * 2. the url in the manifest file complies with RFC-2396
     * 3. updateInterval is less than validForInDays value in the manifest file
     *
     * @param url the url to validate
     * @param errors the errors to add error messages
     */
    private void validateManifestFile(final URL url, final ActionRequestValidationException errors) {
        DatasourceManifest manifest;
        try {
            manifest = DatasourceManifest.Builder.build(url);
        } catch (Exception e) {
            log.info("Error occurred while reading a file from {}", url, e);
            errors.addValidationError(String.format(Locale.ROOT, "Error occurred while reading a file from %s: %s", url, e.getMessage()));
            return;
        }

        try {
            new URL(manifest.getUrl()).toURI(); // Validate URL complies with RFC-2396
        } catch (MalformedURLException | URISyntaxException e) {
            log.info("Invalid URL[{}] is provided for url field in the manifest file", manifest.getUrl(), e);
            errors.addValidationError("Invalid URL format is provided for url field in the manifest file");
            return;
        }

        if (manifest.getValidForInDays() != null && updateInterval.days() >= manifest.getValidForInDays()) {
            errors.addValidationError(
                String.format(
                    Locale.ROOT,
                    "updateInterval %d should be smaller than %d",
                    updateInterval.days(),
                    manifest.getValidForInDays()
                )
            );
        }
    }

    /**
     * Validate updateInterval is equal or larger than 1
     *
     * @param errors the errors to add error messages
     */
    private void validateUpdateInterval(final ActionRequestValidationException errors) {
        if (updateInterval.compareTo(TimeValue.timeValueDays(1)) < 0) {
            errors.addValidationError("Update interval should be equal to or larger than 1 day");
        }
    }
}
