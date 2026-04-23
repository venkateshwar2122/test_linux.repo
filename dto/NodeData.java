package com.example.nifi.dto;

import java.util.List;

public class NodeData {

    private Connection connection;
    private List<Schema> schema;

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public List<Schema> getSchema() {
        return schema;
    }

    public void setSchema(List<Schema> schema) {
        this.schema = schema;
    }
}
