package org.apache.storm.starter.service;

import org.apache.log4j.Logger;
import org.apache.storm.starter.mongodb.MorphiaSingleton;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * Created by zonghan on 7/22/16.
 */
public class TableUtil {

    private static final Logger log = Logger.getLogger(TableUtil.class);

    private static Connection getConnection(ConnectionInfo connectionInfo) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException ex) {
            log.error("Error: unable to load driver class!");
        }
        try {
            Connection con = DriverManager.getConnection
                    (connectionInfo.getUrl(), connectionInfo.getUsername(),
                            connectionInfo.getPassword());
            return con;
        } catch (SQLException e) {
            log.error(e);
        }
        return null;
    }

    public static void migrateTableData(String tableName, String sourceSchema, ConnectionInfo sourceConnInfo,
                                        String targetSchema, ConnectionInfo targetConnInfo)
            throws SQLException, InterruptedException {
        int rows = 0;
        int batchSize = 300;
        String queryString = "SELECT * FROM " + sourceSchema + "." + tableName + "";
        PreparedStatement pstmt = null;
        PreparedStatement prepStmnt = null;
        ResultSet queryResult = null;
        String insertString = null;
        Connection sourceConn = getConnection(sourceConnInfo);
        Connection targetConn = null;
        try {
            pstmt = sourceConn.prepareStatement(queryString);
            pstmt.setFetchSize(300);
            queryResult = pstmt.executeQuery();
            ResultSetMetaData md = queryResult.getMetaData();

            String columns = "";
            for (int j = 0; j < md.getColumnCount(); j++) {
                columns = columns + "?,";
            }
            columns = columns.substring(0, columns.length() - 1);

            insertString = "INSERT INTO " + targetSchema + "." + tableName
                    + " VALUES (" + columns + ")";
            if (sourceConnInfo.equals(targetConnInfo)) {
                // prevent from dead lock
                targetConn = sourceConn;
            } else {
                targetConn = getConnection(targetConnInfo);
            }
            prepStmnt = targetConn.prepareStatement(insertString);

            int cols = md.getColumnCount();

            while (queryResult.next()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException(
                            "migrateTableData thread is interrupted");
                }

                for (int i = 1; i <= cols; i++) {
                    int type = md.getColumnType(i);
                    if (type == Types.DECIMAL || type == Types.DOUBLE
                            || type == Types.NUMERIC) {

                        if (type == Types.NUMERIC) {
                            BigDecimal d = queryResult.getBigDecimal(i);
                            if (queryResult.wasNull()) {
                                prepStmnt.setNull(i, type);
                            } else {
                                if (d.precision() > 34
                                        && !tableName.contains("EMAIL_TEMPLATE")) {
                                    prepStmnt.setBigDecimal(i, new BigDecimal(0));
                                } else if (md.getPrecision(i) == 126) {
                                    prepStmnt.setFloat(i, d.floatValue());
                                } else {
                                    prepStmnt.setBigDecimal(i, d);
                                }

                            }
                        } else {
                            double d = queryResult.getDouble(i);
                            if (queryResult.wasNull()) {
                                prepStmnt.setNull(i, type);
                            } else {
                                prepStmnt.setDouble(i, d);
                            }
                        }

                    } else if (type == Types.INTEGER) {
                        int n = queryResult.getInt(i);
                        if (queryResult.wasNull()) {
                            prepStmnt.setNull(i, type);
                        } else {
                            prepStmnt.setInt(i, n);
                        }

                    } else if (type == Types.SMALLINT) {
                        short n = queryResult.getShort(i);
                        if (queryResult.wasNull()) {
                            prepStmnt.setNull(i, type);
                        } else {
                            prepStmnt.setShort(i, n);
                        }

                    } else if (type == Types.VARCHAR) {
                        String s = queryResult.getString(i);
                        prepStmnt.setString(i, s);
                    } else if (type == Types.NVARCHAR || type == Types.CHAR) {
                        String s = queryResult.getString(i);
                        prepStmnt.setString(i, s);
                    } else if (type == Types.DATE) {
                        Date day = queryResult.getDate(i);
                        prepStmnt.setDate(i, day);
                    } else if (type == Types.TIME) {
                        Time time = queryResult.getTime(i);
                        prepStmnt.setTime(i, time);
                    } else if (type == Types.TIMESTAMP) {
                        Timestamp timestamp = queryResult.getTimestamp(i);

                        prepStmnt.setTimestamp(i, timestamp);
                        if (timestamp != null) {
                            Calendar cal = Calendar.getInstance();
                            cal.setTimeInMillis(timestamp.getTime());
                            int realyear = cal.get(Calendar.YEAR);

                            if (realyear == 0 || realyear < -4713 || realyear > 9999) {
                                prepStmnt.setTimestamp(i, null);
                            }
                        }

                    } else if (type == Types.CLOB) {
                        String nclob = queryResult.getString(i);
                        prepStmnt.setString(i, nclob);

                    } else if (type == Types.BLOB) {
                        Blob blob = queryResult.getBlob(i);
                        if (blob == null) {
                            prepStmnt.setNull(i, Types.BLOB);
                        } else {
                            long lenght = blob.length();
                            byte[] bytes = new byte[Long.valueOf(lenght).intValue()];
                            try {
                                blob.getBinaryStream().read(bytes);
                            } catch (IOException e) {
                                log.error(e);
                            }
                            prepStmnt.setBytes(i, bytes);
                        }
                    } else if (type == Types.VARBINARY || type == Types.BINARY) {
                        byte[] s = queryResult.getBytes(i);
                        prepStmnt.setBytes(i, s);
                    } else {
                        byte[] s = queryResult.getBytes(i);
                        prepStmnt.setBytes(i, s);
                    }
                }
                rows++;
                prepStmnt.addBatch();
                log.info(targetSchema + "add batch:" + insertString);

                if (batchSize > 0) {
                    try {
                        if (rows % batchSize == 0) {
                            prepStmnt.executeBatch();
                            //targetConn.commit();
                            log.info(targetSchema + "batch executed!");
                            prepStmnt.clearBatch();
                        }
                    } catch (SQLException e) {
                        log.error(e);
                    }
                }
            }
            prepStmnt.executeBatch();
            //targetConn.commit();
            log.info(targetSchema + "batch executed!");
            prepStmnt.clearBatch();
            columns = null;
        } catch (SQLException e) {
            log.error(e);
        } catch (Exception e) {
            log.error("exception caught when copying data of table:" + tableName);
            log.error(e);
        } finally {
            queryResult.close();
            pstmt.close();
            prepStmnt.close();
            targetConn.close();
            sourceConn.close();
        }
    }
}
