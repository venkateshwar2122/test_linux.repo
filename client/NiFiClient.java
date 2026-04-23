package com.example.nifi.client;

import com.example.nifi.config.NiFiProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

@Component
public class NiFiClient {

    private final RestTemplate restTemplate;
    private final NiFiProperties nifiProps; // ✅ renamed (IMPORTANT)
    private final ObjectMapper mapper = new ObjectMapper();

    public NiFiClient(RestTemplate restTemplate, NiFiProperties nifiProps) {
        this.restTemplate = restTemplate;
        this.nifiProps = nifiProps;
    }

    // 🔐 GET TOKEN
    public String getToken() {

        String url = nifiProps.getBaseUrl() + "/access/token";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();

        // ✅ DO NOT ENCODE
        body.add("username", nifiProps.getUsername());
        body.add("password", nifiProps.getPassword());

        HttpEntity<MultiValueMap<String, String>> request =
                new HttpEntity<>(body, headers);

        return restTemplate.postForObject(url, request, String.class);
    }

    // 📦 CREATE PROCESS GROUP
    public String createPG(String token, String rootId, String name) {

        String url = nifiProps.getBaseUrl() + "/process-groups/" + rootId + "/process-groups";

        String requestBody = """
        {
          "revision":{"version":0},
          "component":{"name":"%s"}
        }
        """.formatted(name);

        JsonNode res = exchange(url, HttpMethod.POST, token, requestBody);

        return res.get("component").get("id").asText();
    }

    // ⚙️ CREATE CONTROLLER SERVICE
    public String createCS(String token, String pgId, String type) {

        String url = nifiProps.getBaseUrl() + "/process-groups/" + pgId + "/controller-services";

        String requestBody = """
        {
          "revision":{"version":0},
          "component":{"type":"%s"}
        }
        """.formatted(type);

        JsonNode res = exchange(url, HttpMethod.POST, token, requestBody);

        return res.get("component").get("id").asText();
    }

    // 🔄 UPDATE CONTROLLER SERVICE
    public void updateCS(String token, String id, int version, String propertiesJson) {

        String url = nifiProps.getBaseUrl() + "/controller-services/" + id;

        String requestBody = """
        {
          "revision":{"version":%d},
          "component":{
            "id":"%s",
            "properties":%s
          }
        }
        """.formatted(version, id, propertiesJson);

        exchange(url, HttpMethod.PUT, token, requestBody);
    }

    // ▶️ ENABLE CONTROLLER SERVICE
    public void enable(String token, String id, int version) {

        String url = nifiProps.getBaseUrl() + "/controller-services/" + id + "/run-status";

        String requestBody = """
        {
          "revision":{"version":%d},
          "state":"ENABLED"
        }
        """.formatted(version);

        exchange(url, HttpMethod.PUT, token, requestBody);
    }

    // ⚙️ CREATE PROCESSOR
    public String createProcessor(String token, String pgId, String type) {

        String url = nifiProps.getBaseUrl() + "/process-groups/" + pgId + "/processors";

        String requestBody = """
        {
          "revision":{"version":0},
          "component":{"type":"%s"}
        }
        """.formatted(type);

        JsonNode res = exchange(url, HttpMethod.POST, token, requestBody);

        return res.get("component").get("id").asText();
    }

    // 🔄 UPDATE PROCESSOR
    public void updateProcessorFull(
            String token,
            String id,
            int version,
            String propertiesJson,
            String schedule,
            String autoTerminate
    ) {

        String url = nifiProps.getBaseUrl() + "/processors/" + id;

        String requestBody = """
        {
          "revision":{"version":%d},
          "component":{
            "id":"%s",
            "config":{
              "properties":%s,
              "schedulingPeriod":"%s",
              "autoTerminatedRelationships":%s
            }
          }
        }
        """.formatted(version, id, propertiesJson, schedule, autoTerminate);

        exchange(url, HttpMethod.PUT, token, requestBody);
    }

    // 🔗 CONNECT PROCESSORS
    public void connect(String token, String pgId, String sourceId, String destId) {

        String url = nifiProps.getBaseUrl() + "/process-groups/" + pgId + "/connections";

        String requestBody = """
        {
          "revision":{"version":0},
          "component":{
            "source":{"id":"%s"},
            "destination":{"id":"%s"},
            "selectedRelationships":["success"]
          }
        }
        """.formatted(sourceId, destId);

        exchange(url, HttpMethod.POST, token, requestBody);
    }

    // ▶️ / ⛔ CONTROL FLOW
    public void controlProcessGroup(String token, String pgId, String state) {

        String url = nifiProps.getBaseUrl() + "/flow/process-groups/" + pgId;

        String requestBody = """
        {
          "id":"%s",
          "state":"%s"
        }
        """.formatted(pgId, state);

        exchange(url, HttpMethod.PUT, token, requestBody);
    }

    // 🔍 GET VERSION (REVISION)
    public int getVersion(String token, String id, String type) {

        String url = nifiProps.getBaseUrl() + "/" + type + "/" + id;

        JsonNode res = exchange(url, HttpMethod.GET, token, null);

        return res.get("revision").get("version").asInt();
    }

    // 🔥 COMMON METHOD
    private JsonNode exchange(String url, HttpMethod method, String token, String body) {

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setBearerAuth(token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = new HttpEntity<>(body, headers);

            ResponseEntity<String> response =
                    restTemplate.exchange(url, method, entity, String.class);

            return mapper.readTree(response.getBody());

        } catch (Exception e) {
            throw new RuntimeException("NiFi API error: " + e.getMessage(), e);
        }
    }
}
