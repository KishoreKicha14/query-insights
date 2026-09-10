/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

/**
 * Backwards-compatibility tests for the query-source attributes added in 3.9.0. A node older than
 * 3.9.0 reads attributes by enum name and would fail on an unknown value, so these attributes must
 * not be streamed to such a node.
 */
public class QuerySourceBwcTests extends OpenSearchTestCase {

    private SearchQueryRecord recordWithQuerySourceAttributes() {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(5L));
        measurements.put(MetricType.CPU, new Measurement(10L));
        measurements.put(MetricType.MEMORY, new Measurement(20L));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.DERIVED_FROM, "PPL:node-1:42");
        attributes.put(Attribute.PARENT_MARKER, "PPL:node-1:42");
        attributes.put(Attribute.IS_CHILD, true);
        attributes.put(Attribute.NODE_ID, "node-1");

        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "id-1");
    }

    public void testNewAttributesStrippedForOldNode() throws IOException {
        SearchQueryRecord record = recordWithQuerySourceAttributes();
        Version oldVersion = VersionUtils.getPreviousVersion(Version.V_3_9_0);

        SearchQueryRecord out = roundTrip(record, oldVersion);

        // The pre-3.9.0 attribute survives; the new query-source attributes are dropped so an old
        // node never sees an unknown enum name.
        assertEquals("node-1", out.getAttributes().get(Attribute.NODE_ID));
        assertNull(out.getAttributes().get(Attribute.QUERY_SOURCE));
        assertNull(out.getAttributes().get(Attribute.DERIVED_FROM));
        assertNull(out.getAttributes().get(Attribute.PARENT_MARKER));
        assertNull(out.getAttributes().get(Attribute.IS_CHILD));
    }

    public void testNewAttributesPreservedForCurrentNode() throws IOException {
        SearchQueryRecord record = recordWithQuerySourceAttributes();

        SearchQueryRecord out = roundTrip(record, Version.V_3_9_0);

        assertEquals("PPL", out.getAttributes().get(Attribute.QUERY_SOURCE));
        assertEquals("PPL:node-1:42", out.getAttributes().get(Attribute.DERIVED_FROM));
        assertEquals("PPL:node-1:42", out.getAttributes().get(Attribute.PARENT_MARKER));
        assertEquals(true, out.getAttributes().get(Attribute.IS_CHILD));
    }

    private static SearchQueryRecord roundTrip(SearchQueryRecord record, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            record.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                return new SearchQueryRecord(in);
            }
        }
    }
}
