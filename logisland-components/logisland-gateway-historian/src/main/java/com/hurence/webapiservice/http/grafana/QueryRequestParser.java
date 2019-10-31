package com.hurence.webapiservice.http.grafana;

import com.hurence.webapiservice.http.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.grafana.modele.Target;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class QueryRequestParser {



    private static SimpleDateFormat dateFormat = createDateFormat();

    private static SimpleDateFormat createDateFormat() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        TimeZone myTimeZone = TimeZone.getTimeZone(ZoneId.of("UTC"));
        dateFormat.setTimeZone(myTimeZone);
        dateFormat.setLenient(false);
        return dateFormat;
    }

    public QueryRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        QueryRequestParam.Builder builder = new QueryRequestParam.Builder();
        long from = parseFrom(requestBody);
        builder.from(from);
        long to = parseTo(requestBody);
        builder.to(to);
        String format = parseFormat(requestBody);
        builder.withFormat(format);
        int maxDataPoints = parseMaxDataPoints(requestBody);;
        builder.withMaxDataPoints(maxDataPoints);
        List<Target> targets = parseTargets(requestBody);;
        builder.withTargets(targets);
        return builder.build();
    }

    private List<Target> parseTargets(JsonObject requestBody) {
        return requestBody.getJsonArray("targets").stream()
                .map(JsonObject.class::cast)
                .map(JsonObject::encode)
                .map(json -> Json.decodeValue(json, Target.class))
                .collect(Collectors.toList());
    }

    private int parseMaxDataPoints(JsonObject requestBody) {
        return requestBody.getInteger("maxDataPoints");
    }

    private String parseFormat(JsonObject requestBody) {
        return requestBody.getString("format");
    }

    private long parseDate(JsonObject requestBody, String pointer) {
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj instanceof String) {
            try {
                return dateFormat.parse((String) fromObj).getTime();
            } catch (ParseException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be parsed as a valid date !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not a string !",
                        pointer, fromObj));
    }

    private long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, "/range/from");
    }

    private long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, "/range/to");
    }
}
