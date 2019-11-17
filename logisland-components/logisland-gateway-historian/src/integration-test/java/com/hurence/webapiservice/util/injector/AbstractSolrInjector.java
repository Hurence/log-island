package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import com.hurence.util.modele.ChunkModele;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hurence.webapiservice.historian.HistorianFields.*;

public abstract class AbstractSolrInjector implements SolrInjector {

    protected int ddcThreshold = 0;
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION;
    private List<ChunkModele> extraCustomChunks = new ArrayList<>();

    protected byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    @Override
    public void injectChunks(SolrClient client) throws SolrServerException, IOException {
        final List<ChunkModele> chunks = buildListOfChunks();
        chunks.addAll(extraCustomChunks);
        for(int i = 0; i < chunks.size(); i++) {
            ChunkModele chunkExpected = chunks.get(i);
            client.add(COLLECTION, buildSolrDocument(chunkExpected, "id" + i));
        }
        UpdateResponse updateRsp = client.commit(COLLECTION);
    }

    public void addChunk(ChunkModele chunk) {
        extraCustomChunks.add(chunk);
    }

    public void addChunk(AbstractSolrInjector injector) {
        extraCustomChunks.addAll(injector.buildListOfChunks());
    }

    protected abstract List<ChunkModele> buildListOfChunks();

    private SolrInputDocument buildSolrDocument(ChunkModele chunk, String id) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(RESPONSE_CHUNK_ID_FIELD, id);
        doc.addField(RESPONSE_CHUNK_START_FIELD, chunk.start);
        doc.addField(RESPONSE_CHUNK_SIZE_FIELD, chunk.points.size());
        doc.addField(RESPONSE_CHUNK_END_FIELD, chunk.end);
        doc.addField(RESPONSE_CHUNK_SAX_FIELD, chunk.sax);
        doc.addField(RESPONSE_CHUNK_VALUE_FIELD, chunk.compressedPoints);
        doc.addField(RESPONSE_CHUNK_AVG_FIELD, chunk.avg);
        doc.addField(RESPONSE_CHUNK_MIN_FIELD, chunk.min);
        doc.addField(RESPONSE_CHUNK_WINDOW_MS_FIELD, 11855);
        doc.addField(RESPONSE_METRIC_NAME_FIELD, chunk.name);
        doc.addField(RESPONSE_CHUNK_TREND_FIELD, chunk.trend);
        doc.addField(RESPONSE_CHUNK_MAX_FIELD, chunk.max);
        doc.addField(RESPONSE_CHUNK_SIZE_BYTES_FIELD, chunk.compressedPoints.length);
        doc.addField(RESPONSE_CHUNK_SUM_FIELD, chunk.sum);
        doc.addField(RESPONSE_TAG_NAME_FIELD, chunk.tags);
        doc.addField(RESPONSE_CHUNK_FIRST_VALUE_FIELD, chunk.points.get(0).getValue());

        return doc;
    }
}