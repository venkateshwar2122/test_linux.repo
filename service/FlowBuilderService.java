package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.config.NiFiProperties;
import com.example.nifi.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;

@Service
public class FlowBuilderService {

    private static final Logger log = LoggerFactory.getLogger(FlowBuilderService.class);

    private final NiFiClient client;
    private final ControllerServiceManager cs;
    private final ProcessorManager pm;
    private final NiFiProperties nifi;

    public FlowBuilderService(NiFiClient client,
                              ControllerServiceManager cs,
                              ProcessorManager pm,
                              NiFiProperties nifi) {
        this.client = client;
        this.cs = cs;
        this.pm = pm;
        this.nifi = nifi;
    }

    public String buildFlow(FlowRequest request) {

        try {
            log.info("🚀 Starting dynamic flow creation...");

            // 🔐 1. GET TOKEN
            String token = client.getToken();

            // 📦 2. CREATE PROCESS GROUP
            String pgId = client.createPG(token, nifi.getRootGroupId(), request.getDatastreamName());

            // 🔍 3. EXTRACT SOURCE & TARGET CONFIG SAFELY
            Config source = null;
            Config target = null;

            for (StreamNode node : request.getStreamNodes()) {

                if (node.getData() != null &&
                        node.getData().getConnection() != null &&
                        node.getData().getConnection().getConfig() != null) {

                    Config config = node.getData().getConnection().getConfig();

                    if ("mysql".equalsIgnoreCase(config.getDbType())) {
                        source = config;
                    }

                    if ("mongodb".equalsIgnoreCase(config.getDbType())) {
                        target = config;
                    }
                }
            }

            if (source == null || target == null) {
                throw new RuntimeException("Source or Target DB config missing");
            }

            // 🧪 4. VALIDATE MYSQL CONNECTION BEFORE BUILDING FLOW
            validateMySqlConnection(source);

            // 📊 5. EXTRACT TABLE NAME SAFELY
            String tableName = null;

            for (StreamNode node : request.getStreamNodes()) {
                if (node.getData() != null && node.getData().getSchema() != null) {

                    for (Schema schema : node.getData().getSchema()) {

                        if (schema.getTables() != null && !schema.getTables().isEmpty()) {
                            tableName = schema.getTables().get(0).getTableName();
                            break;
                        }
                    }
                }
            }

            if (tableName == null) {
                throw new RuntimeException("No table found in request");
            }

            log.info("📊 Table detected: {}", tableName);

            // ⚙️ 6. CREATE CONTROLLER SERVICES
            String dbcpId = cs.createDbcp(token, pgId, source);
            String writerId = cs.createJsonWriter(token, pgId);
            String readerId = cs.createJsonReader(token, pgId);
            String mongoId = cs.createMongo(token, pgId, target);

            // 🔧 7. CREATE PROCESSORS
            String queryId = pm.createQueryProcessor(
                    token, pgId, dbcpId, writerId, tableName
            );

            String putMongoId = pm.createPutMongoProcessor(
                    token, pgId, mongoId, readerId, tableName, target.getDatabase()
            );
            // 🔗 8. CONNECT PROCESSORS
            client.connect(token, pgId, queryId, putMongoId);

            // ▶️ 9. START FLOW
            client.controlProcessGroup(token, pgId, "RUNNING");

            return "✅ FLOW CREATED | PG_ID=" + pgId;

        } catch (Exception e) {
            log.error("❌ Flow creation failed", e);
            throw new RuntimeException("Flow creation failed: " + e.getMessage(), e);
        }
    }

    // 🧪 MYSQL CONNECTION VALIDATION
    private void validateMySqlConnection(Config config) {

        try {
            String url = "jdbc:mysql://" +
                    config.getHost() + ":" +
                    config.getPort() + "/" +
                    config.getDatabase();

            Connection conn = DriverManager.getConnection(
                    url,
                    config.getUsername(),
                    config.getPassword()
            );

            conn.close();

            log.info("✅ MySQL connection successful");

        } catch (Exception e) {
            throw new RuntimeException("MySQL connection failed: " + e.getMessage());
        }
    }

    // ▶️ START FLOW
    public void startFlow(String pgId) {
        String token = client.getToken();
        client.controlProcessGroup(token, pgId, "RUNNING");
    }

    // ⛔ STOP FLOW
    public void stopFlow(String pgId) {
        String token = client.getToken();
        client.controlProcessGroup(token, pgId, "STOPPED");
    }
}
