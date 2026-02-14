package org.rancidcode.telemetrycollector.domain;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class TelemetryValidator {

    public JSONObject getMessage(String message) {
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(message);
        } catch (JSONException e) {
            jsonObject = buildDlq(e, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return jsonObject;
    }

    private JSONObject buildDlq(JSONException e, String message) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);

        JSONObject dlq = new JSONObject();
        dlq.put("timestamp", now.toString());
        dlq.put("dlqType", "INVALID_JSON");
        dlq.put("source", "MQTT");
        dlq.put("rawMessage", message);
        dlq.put("errorMessage", e.getMessage());


        return dlq;
    }
}
