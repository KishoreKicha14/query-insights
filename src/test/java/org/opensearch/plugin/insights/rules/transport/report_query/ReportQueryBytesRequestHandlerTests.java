/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.rules.transport.report_query;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.mockito.ArgumentCaptor;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.report_query.ReportQueryBytesAction;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportChannel;

/**
 * Tests for {@link ReportQueryBytesRequestHandler}, focused on the wire deserialization producing
 * a {@link SearchQueryRecord} with the attributes the SQL producer sends — in particular the
 * {@code failed} flag mapping to {@link Attribute#FAILED} so Top N classifies failed PPL/SQL
 * queries correctly.
 */
public class ReportQueryBytesRequestHandlerTests extends OpenSearchTestCase {

    private SearchQueryRecord ingestRecordFor(final boolean failed) throws Exception {
        final QueryInsightsService service = mock(QueryInsightsService.class);
        final ReportQueryBytesRequestHandler handler = new ReportQueryBytesRequestHandler(service);

        final BytesTransportRequest request = new BytesTransportRequest(serializePayload(failed), Version.CURRENT);
        final TransportChannel channel = mock(TransportChannel.class);

        handler.messageReceived(request, channel, mock(Task.class));

        final ArgumentCaptor<SearchQueryRecord> captor = ArgumentCaptor.forClass(SearchQueryRecord.class);
        verify(service).addRecord(captor.capture());
        return captor.getValue();
    }

    /**
     * Build a payload identical in field order to what the SQL plugin's {@code QueryInsightsReporter}
     * writes, so this test guards the producer/consumer wire contract.
     */
    private org.opensearch.core.common.bytes.BytesReference serializePayload(final boolean failed) throws Exception {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeVInt(ReportQueryBytesAction.FORMAT_VERSION);
        out.writeString("PPL");
        out.writeString("PPL:node-1:42");
        out.writeString("node-1");
        out.writeString("source=my_index | head 10");
        out.writeVLong(1_700_000_000_000L);
        out.writeVLong(15L);
        out.writeVLong(1_000L);
        out.writeVLong(2_048L);
        final List<String> indices = List.of("my_index");
        out.writeVInt(indices.size());
        for (final String idx : indices) {
            out.writeString(idx);
        }
        out.writeString("");
        out.writeBoolean(failed);
        return out.bytes();
    }

    public void testFailedFlagTrueSetsFailedAttribute() throws Exception {
        final SearchQueryRecord record = ingestRecordFor(true);
        assertEquals(true, record.getAttributes().get(Attribute.FAILED));
    }

    public void testFailedFlagFalseSetsFailedAttribute() throws Exception {
        final SearchQueryRecord record = ingestRecordFor(false);
        assertEquals(false, record.getAttributes().get(Attribute.FAILED));
    }
}
