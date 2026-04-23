package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.constants.FlowConstants;
import com.example.nifi.dto.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ControllerServiceManager {

    private static final Logger log = LoggerFactory.getLogger(ControllerServiceManager.class);

    private final NiFiClient client;

    public ControllerServiceManager(NiFiClient client) {
        this.client = client;
    }

    // 🔧 COMMON SETUP METHOD
    private String setup(String token, String pgId, String type, String propertiesJson) {

        log.info("⚙️ Creating Controller Service: {}", type);

        String id = client.createCS(token, pgId, type);

        int version = client.getVersion(token, id, "controller-services");

        client.updateCS(token, id, version, propertiesJson);

        int newVersion = client.getVersion(token, id, "controller-services");

        client.enable(token, id, newVersion);

        log.info("✅ Controller Service Enabled: {}", id);

        return id;
    }

    // 🔥 DBCP (MYSQL)
    public String createDbcp(String token, String pgId, Config config) {

        String jdbcUrl = "jdbc:mysql://" +
                config.getHost() + ":" +
                config.getPort() + "/" +
                config.getDatabase();

        String propertiesJson = """
        {
          "Database Connection URL":"%s",
          "Database Driver Class Name":"%s",
          "Database Driver Locations":"%s",
          "Database User":"%s",
          "Password":"%s"
        }
        """.formatted(
                jdbcUrl,
                FlowConstants.MYSQL_DRIVER,
                FlowConstants.MYSQL_DRIVER_PATH,
                config.getUsername(),
                config.getPassword()
        );

        return setup(
                token,
                pgId,
                "org.apache.nifi.dbcp.DBCPConnectionPool",
                propertiesJson
        );
    }

    // 🔥 JSON WRITER
    public String createJsonWriter(String token, String pgId) {

        return setup(
                token,
                pgId,
                "org.apache.nifi.json.JsonRecordSetWriter",
                "{}"
        );
    }

    // 🔥 JSON READER
    public String createJsonReader(String token, String pgId) {

        String propertiesJson = """
        {
          "Schema Access Strategy": "infer-schema"
        }
        """;

        return setup(
                token,
                pgId,
                "org.apache.nifi.json.JsonTreeReader",
                propertiesJson
        );
    }

    // 🔥 MONGO CONTROLLER SERVICE
    public String createMongo(String token, String pgId, Config config) {

        String mongoUri = "mongodb://" +
                config.getUsername() + ":" +
                config.getPassword() + "@" +
                config.getHost() + ":" +
                config.getPort();

        String propertiesJson = """
        {
          "Mongo URI":"%s"
        }
        """.formatted(mongoUri);

        return setup(
                token,
                pgId,
                "org.apache.nifi.mongodb.MongoDBControllerService",
                propertiesJson
        );
    }
}





