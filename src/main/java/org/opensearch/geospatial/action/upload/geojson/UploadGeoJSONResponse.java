/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

//UploadGeoJSONResponse represents UploadGeoJSONRequest's Response
public class UploadGeoJSONResponse extends ActionResponse implements ToXContentObject {
    private static final String ERRORS = "errors";
    private static final String FAILURE = "failure";
    private static final String FAILURES = "failures";
    private static final String ID = "id";
    private static final String MESSAGE = "message";
    private static final int NO_FAILURE = 0;
    private static final String STATUS = "status";
    private static final String SUCCESS = "success";
    private static final String TOTAL = "total";
    private static final String TOOK = "took";

    private final BulkResponse bulkResponse;

    public UploadGeoJSONResponse(BulkResponse bulkResponse) {
        super();
        this.bulkResponse = bulkResponse;
    }

    public UploadGeoJSONResponse(StreamInput in) throws IOException {
        super(in);
        this.bulkResponse = new BulkResponse(in);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        this.bulkResponse.writeTo(streamOutput);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        /*
        If BulkResponse has no failures:
            {
              "took": 100,
              "errors": false,
              "total": 5,
              "success": 5,
              "failure": 0
            }
        If BulkResponse has failures:
            {
              "took": 100,
              "errors": true,
              "total": 4,
              "success": 2,
              "failure": 2,
              "failures": [
                  {
                    "id" : "DocId2",
                    "message" : "failed to index due to ..."
                  },
                  {
                    "id" : "DocId3",
                    "message" : "failed to index due to ..."
                  }
             ]
          }
         */
        builder.startObject();
        builder.field(TOOK, bulkResponse.getTook().getMillis());
        builder.field(ERRORS, bulkResponse.hasFailures());
        builder.field(TOTAL, bulkResponse.getItems().length);
        if (!bulkResponse.hasFailures()) {
            buildSuccessXContent(builder);
            return builder.endObject();
        }
        buildFailureXContent(builder);
        return builder.endObject();
    }

    private void buildSuccessXContent(XContentBuilder builder) throws IOException {
        buildResultXContent(builder, bulkResponse.getItems().length, NO_FAILURE);
    }

    private void buildResultXContent(XContentBuilder builder, int successCount, int failureCount) throws IOException {
        builder.field(SUCCESS, successCount);
        builder.field(FAILURE, failureCount);
    }

    private void buildFailureXContent(XContentBuilder builder) throws IOException {
        final List<BulkItemResponse> failedResponses = Arrays.stream(bulkResponse.getItems())
            .filter(BulkItemResponse::isFailed)
            .collect(Collectors.toUnmodifiableList());
        int successCount = bulkResponse.getItems().length - failedResponses.size();
        buildResultXContent(builder, successCount, failedResponses.size());
        builder.startArray(FAILURES);
        for (BulkItemResponse response : failedResponses) {
            builder.startObject();
            builder.field(ID, response.getId());
            builder.field(STATUS, response.getFailure().getStatus().getStatus());
            builder.field(MESSAGE, response.getFailureMessage());
            builder.endObject();
        }
        builder.endArray();
    }
}
