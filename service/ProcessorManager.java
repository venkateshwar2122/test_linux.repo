package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.constants.FlowConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProcessorManager {

    private static final Logger log = LoggerFactory.getLogger(ProcessorManager.class);

    private final NiFiClient client;

    public ProcessorManager(NiFiClient client) {
        this.client = client;
    }

    // 🔥 QUERY DATABASE PROCESSOR (SOURCE)
    public String createQueryProcessor(
            String token,
            String pgId,
            String dbcpId,
            String writerId,
            String tableName) {

        log.info("⚙️ Creating QueryDatabaseTableRecord processor...");

        String processorId = client.createProcessor(
                token,
                pgId,
                "org.apache.nifi.processors.standard.QueryDatabaseTableRecord"
        );

        int version = client.getVersion(token, processorId, "processors");

        String propertiesJson = """
        {
          "Database Connection Pooling Service": "%s",
          "Table Name": "%s",
          "Record Writer": "%s",
          "Maximum-value Columns": "%s"
        }
        """.formatted(
                dbcpId,
                tableName,
                writerId,
                FlowConstants.PRIMARY_KEY
        );

        client.updateProcessorFull(
                token,
                processorId,
                version,
                propertiesJson,
                FlowConstants.SCHEDULING,
                "[\"" + FlowConstants.FAILURE_RELATIONSHIP + "\"]"
        );

        log.info("✅ Query Processor created: {}", processorId);

        return processorId;
    }

    // 🔥 PUT MONGO PROCESSOR (TARGET)
    public String createPutMongoProcessor(
            String token,
            String pgId,
            String mongoId,
            String readerId,
            String tableName,
            String database) {

        log.info("⚙️ Creating PutMongoRecord processor...");

        String processorId = client.createProcessor(
                token,
                pgId,
                "org.apache.nifi.processors.mongodb.PutMongoRecord"
        );

        int version = client.getVersion(token, processorId, "processors");

        String propertiesJson = """
        {
          "Mongo Client Service": "%s",
          "Mongo Database Name": "%s",
          "Mongo Collection Name": "%s",
          "Record Reader": "%s",
          "Update Key Fields": "%s"
        }
        """.formatted(
                mongoId,
                database,              // ✅ correct DB name
                tableName,             // collection = table
                readerId,
                FlowConstants.PRIMARY_KEY
        );

        client.updateProcessorFull(
                token,
                processorId,
                version,
                propertiesJson,
                FlowConstants.NO_SCHEDULING,
                "[\"" + FlowConstants.FAILURE_RELATIONSHIP + "\"]"
        );

        log.info("✅ Mongo Processor created: {}", processorId);

        return processorId;
    }
}
