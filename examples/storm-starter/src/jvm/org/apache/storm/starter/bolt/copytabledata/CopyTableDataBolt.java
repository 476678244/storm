package org.apache.storm.starter.bolt.copytabledata;

import org.apache.log4j.Logger;
import org.apache.storm.starter.bean.CopyTableDataRequest;
import org.apache.storm.starter.bean.RequestStatusEnum;
import org.apache.storm.starter.mongodb.CopyDataRequestDAO;
import org.apache.storm.starter.mongodb.MorphiaSingleton;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by zonghan on 7/20/16.
 */
public class CopyTableDataBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(CopyTableDataBolt.class);

    private OutputCollector collector;

    private CopyDataRequestDAO dao = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.dao = MorphiaSingleton.getCopyDataRequestDAO();
    }

    @Override
    public void execute(Tuple input) {
        LOG.error("*****Thread" + Thread.currentThread().getName());
        CopyTableDataRequest request = (CopyTableDataRequest) input.getValue(0);
        LOG.info("CopyTableDataBolt get request:" + request);
        this.dao.save(request.setStatus(RequestStatusEnum.PROGRESSING));
        LOG.info("CopyTableDataBolt PROGRESSING");
        this.collector.emit(new Values(request));
        LOG.info("CopyTableDataBolt emit request:" + request);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("request"));
    }
}
