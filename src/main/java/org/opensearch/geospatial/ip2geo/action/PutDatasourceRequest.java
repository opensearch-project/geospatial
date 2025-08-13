/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.geospatial.ip2geo.common.ParameterValidator;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Ip2Geo datasource creation request
 */
@Getter
@Setter
@Log4j2
public class PutDatasourceRequest extends ActionRequest {
    public static final ParseField ENDPOINT_FIELD = new ParseField("endpoint");
    public static final ParseField UPDATE_INTERVAL_IN_DAYS_FIELD = new ParseField("update_interval_in_days");
    private static final ParameterValidator VALIDATOR = new ParameterValidator();

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
        List<String> errorMsgs = VALIDATOR.validateDatasourceName(name);
        if (errorMsgs.isEmpty() == false) {
            errorMsgs.stream().forEach(msg -> errors.addValidationError(msg));
        }
        validateEndpoint(errors);
        validateUpdateInterval(errors);
        return errors.validationErrors().isEmpty() ? null : errors;
    }

    /**
     * Conduct following validation on endpoint
     * 1. endpoint format complies with RFC-2396
     *
     * @param errors the errors to add error messages
     */
    private void validateEndpoint(final ActionRequestValidationException errors) {
        try {
            URL url = new URL(endpoint);
            url.toURI(); // Validate URL complies with RFC-2396
        } catch (MalformedURLException | URISyntaxException e) {
            log.info("Invalid URL[{}] is provided", endpoint, e);
            errors.addValidationError("Invalid URL format is provided");
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
