package com.hurence.util.modele;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.List;

import static com.hurence.webapiservice.historian.HistorianFields.*;

public class ChunkModele {
    private static int ddcThreshold = 0;

    public List<Point> points;
    public byte[] compressedPoints;
    public long start;
    public long end;
    public double avg;
    public double min;
    public double max;
    public double sum;
    public boolean trend;
    public String name;
    public String sax;
    public List<String> tags;

public static ChunkModele fromPoints(String metricName, List<Point> points) {
    ChunkModele chunk = new ChunkModele();
    chunk.points = points;
    chunk.compressedPoints = compressPoints(chunk.points);
    chunk.start = chunk.points.stream().mapToLong(Point::getTimestamp).min().getAsLong();
    chunk.end = chunk.points.stream().mapToLong(Point::getTimestamp).max().getAsLong();;
    chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
    chunk.avg = chunk.sum / chunk.points.size();
    chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
    chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
    chunk.name = metricName;
    chunk.sax = "edeebcccdf";
    return chunk;
}

    protected static byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }


    public JsonObject toJson(String id) {
        JsonObject json = new JsonObject();
        json.put(RESPONSE_CHUNK_ID_FIELD, id);
        json.put(RESPONSE_CHUNK_START_FIELD, this.start);
        json.put(RESPONSE_CHUNK_SIZE_FIELD, this.points.size());
        json.put(RESPONSE_CHUNK_END_FIELD, this.end);
        json.put(RESPONSE_CHUNK_SAX_FIELD, this.sax);
        json.put(RESPONSE_CHUNK_VALUE_FIELD, this.compressedPoints);
        json.put(RESPONSE_CHUNK_AVG_FIELD, this.avg);
        json.put(RESPONSE_CHUNK_MIN_FIELD, this.min);
        json.put(RESPONSE_CHUNK_WINDOW_MS_FIELD, 11855);
        json.put(RESPONSE_METRIC_NAME_FIELD, this.name);
        json.put(RESPONSE_CHUNK_TREND_FIELD, this.trend);
        json.put(RESPONSE_CHUNK_MAX_FIELD, this.max);
        json.put(RESPONSE_CHUNK_SIZE_BYTES_FIELD, this.compressedPoints.length);
        json.put(RESPONSE_CHUNK_SUM_FIELD, this.sum);
        json.put(RESPONSE_TAG_NAME_FIELD, this.tags);
        return json;
    }

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
        return doc;
    }
}