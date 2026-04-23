package com.example.nifi.dto;

public class StreamNode {

    private String id;
    private NodeData data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public NodeData getData() {
        return data;
    }

    public void setData(NodeData data) {
        this.data = data;
    }
}
