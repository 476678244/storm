package org.apache.storm.starter.spout.copytabledata;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.bean.CopyTableDataRequest;
import org.apache.storm.starter.bean.RequestStatusEnum;
import org.apache.storm.starter.mongodb.CopyDataRequestDAO;
import org.apache.storm.starter.mongodb.MorphiaSingleton;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by zonghan on 7/20/16.
 */
public class RequestListenSpout extends BaseRichSpout {

    public static int PARALLELISM = 10;

    private static final Logger LOG = LoggerFactory.getLogger(RequestListenSpout.class);

    private SpoutOutputCollector collector;

    private CopyDataRequestDAO dao = null;

    private Datastore ds = null;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.dao = MorphiaSingleton.getCopyDataRequestDAO();
        this.ds = MorphiaSingleton.getDatastore();
    }

    @Override
    public void nextTuple() {
        LOG.debug("RequestListenSpout nextTuple()");
//        List<CopyTableDataRequest> requests = this.dao.find(this.dao.createQuery().filter("status", RequestStatusEnum.CREATED)).asList();
//        requests.stream().forEach(request-> {
//            request.setStatus(RequestStatusEnum.FOUND);
//            this.dao.save(request);
//            this.collector.emit(new Values(request));
//        });
        int EMITNUMBER = PARALLELISM;
        Utils.sleep(200);
        long foundCount = this.dao.count(this.dao.createQuery().filter("status", RequestStatusEnum.FOUND));
        long progressingCount = this.dao.count(this.dao.createQuery().filter("status", RequestStatusEnum.PROGRESSING));
        if (foundCount >= 50 && (progressingCount * 4 ) / 3  >= PARALLELISM) {
            return;
        }
//        CopyTableDataRequest request = this.dao.findOne(
//                this.dao.createQuery().filter("status", RequestStatusEnum.CREATED).order("-endId"));
        List<Key<CopyTableDataRequest>> keys = this.dao.createQuery().filter("status", RequestStatusEnum.CREATED).limit(1000).asKeyList();
        Collections.shuffle(keys);
        if (keys.size() < EMITNUMBER) {
            EMITNUMBER = keys.size();
        }
        for (int i = 0; i < EMITNUMBER ; i ++) {
            Key<CopyTableDataRequest> key = keys.get(i);
            CopyTableDataRequest request = this.dao.get((ObjectId)key.getId());
            if (request != null) {
                request.setStatus(RequestStatusEnum.FOUND);
                this.dao.save(request);
                LOG.info("RequestListenSpout emit!");
                this.collector.emit(new Values(request));
            }
        }

        //Utils.sleep(5000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("request"));

    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        LOG.error("msgId:" + msgId);
        LOG.error("***********************");
        LOG.error("***********************");
        LOG.error("RequestListenSpout fail called!");
        LOG.error("***********************");
        LOG.error("***********************");
    }
}
