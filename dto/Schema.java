package com.example.nifi.dto;

import java.util.List;

public class Schema {

    private List<Table> tables;   // ✅ MUST be named tables

    public List<Table> getTables() {   // ✅ REQUIRED
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }
}
