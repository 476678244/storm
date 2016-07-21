package org.apache.storm.starter.bean;

import java.io.Serializable;

/**
 * Created by zonghan on 7/20/16.
 */
public class CopyTableDataRequest implements Serializable{

    private String connectionUrl;

    private String username;

    private String password;

    private String schema;

    private String table;

    public CopyTableDataRequest(){

    }

    public CopyTableDataRequest(String connectionUrl, String username, String password, String schema, String table) {
        this.connectionUrl = connectionUrl;
        this.username = username;
        this.password = password;
        this.schema = schema;
        this.table = table;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return "CopyTableDataRequest{" +
                "connectionUrl='" + connectionUrl + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
