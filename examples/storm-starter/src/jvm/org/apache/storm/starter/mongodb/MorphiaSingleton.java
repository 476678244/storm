package org.apache.storm.starter.mongodb;

import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import org.apache.storm.starter.bean.CopyTableDataRequest;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zonghan on 7/21/16.
 */
public class MorphiaSingleton {

    private static final Logger LOG = Logger.getLogger(MorphiaSingleton.class);

    private static Datastore ds = null;

    private static ReentrantLock dsLock = new ReentrantLock();

    private static CopyDataRequestDAO copyDataRequestDAO = null;

    private static ReentrantLock daoLock = new ReentrantLock();

    private MorphiaSingleton() {

    }

//    private static final String HOSTNAME = "10.59.178.29";
    private static final String HOSTNAME = "127.0.0.1";

    private static final int PORT = 27017;

    public static Datastore getDatastore() {
        dsLock.lock();
        if (ds == null) {
            LOG.error("*****new ds");
            ds = new Morphia().map(CopyTableDataRequest.class).createDatastore(
                    new MongoClient(HOSTNAME, PORT), "copydatarequest");
        }
        dsLock.unlock();
        return ds;
    }

    public static CopyDataRequestDAO getCopyDataRequestDAO() {
        daoLock.lock();
        if (copyDataRequestDAO == null) {
            copyDataRequestDAO = new CopyDataRequestDAO(CopyTableDataRequest.class, getDatastore());
        }
        daoLock.unlock();
        return copyDataRequestDAO;
    }
}
