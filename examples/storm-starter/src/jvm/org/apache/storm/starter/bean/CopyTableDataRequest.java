package org.apache.storm.starter.bean;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.io.Serializable;

/**
 * Created by zonghan on 7/20/16.
 */
@Entity
public class CopyTableDataRequest implements Serializable, Cloneable {

    @Id
    private ObjectId id = null;

    private String connectionUrl;

    private String username;

    private String password;

    private String schema;

    private String table;

    private RequestStatusEnum status = RequestStatusEnum.CREATED;

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

    public CopyTableDataRequest setTable(String table) {
        this.table = table;
        return this;
    }

    public RequestStatusEnum getStatus() {
        return status;
    }

    public CopyTableDataRequest setStatus(RequestStatusEnum status) {
        this.status = status;
        return this;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new CopyTableDataRequest(
                this.connectionUrl, this.username, this.password, this.schema, this.table);
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
