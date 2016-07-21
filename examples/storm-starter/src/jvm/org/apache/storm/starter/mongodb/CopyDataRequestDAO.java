package org.apache.storm.starter.mongodb;

import org.apache.storm.starter.bean.CopyTableDataRequest;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;

/**
 * Created by zonghan on 7/21/16.
 */
public class CopyDataRequestDAO extends BasicDAO<CopyTableDataRequest, ObjectId> {

    public CopyDataRequestDAO(Class<CopyTableDataRequest> entityClass, Datastore ds) {
        super(entityClass, ds);
    }
}
