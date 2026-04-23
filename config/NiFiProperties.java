package com.example.nifi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "nifi")
public class NiFiProperties {

    private String baseUrl;
    private String username;
    private String password;
    private String rootGroupId;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRootGroupId() {
        return rootGroupId;
    }

    public void setRootGroupId(String rootGroupId) {
        this.rootGroupId = rootGroupId;
    }
}
