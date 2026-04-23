package com.example.nifi.dto;

import java.util.List;

public class FlowRequest {

    private String datastreamName;
    private List<StreamNode> streamNodes;

    public String getDatastreamName() {
        return datastreamName;
    }

    public void setDatastreamName(String datastreamName) {
        this.datastreamName = datastreamName;
    }

    public List<StreamNode> getStreamNodes() {
        return streamNodes;
    }

    public void setStreamNodes(List<StreamNode> streamNodes) {
        this.streamNodes = streamNodes;
    }
}
