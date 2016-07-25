package org.apache.storm.starter.connectionpool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.storm.starter.service.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceFactory.class);

    private static Map<ConnectionInfo, DataSource> dataSources = new
            ConcurrentHashMap<ConnectionInfo, DataSource>();

    public static synchronized  DataSource getDataSource(ConnectionInfo connectionInfo) {
        if (dataSources.containsKey(connectionInfo)) {
            return dataSources.get(connectionInfo);
        } else {
            LOG.info("*******new BasicDataSource:" + connectionInfo);
            BasicDataSource ds = new BasicDataSource();
            ds.setDriverClassName("oracle.jdbc.driver.OracleDriver");
            ds.setUsername(connectionInfo.getUsername());
            ds.setPassword(connectionInfo.getPassword());
            ds.setUrl(connectionInfo.getUrl());
            ds.setMinIdle(0);
            ds.setMaxIdle(3);
            ds.setInitialSize(1);
            ds.setMaxTotal(4);
            ds.setMaxOpenPreparedStatements(50);
            dataSources.put(connectionInfo, ds);
            return ds;
        }
    }
}