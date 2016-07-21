package org.apache.storm.starter.spout.copytabledata;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.bean.CopyTableDataRequest;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zonghan on 7/20/16.
 */
public class RequestListenSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(RequestListenSpout.class);
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        CopyTableDataRequest request = new CopyTableDataRequest(
                "jdbc:1521:dbpool1", "sfuser", "sfuser", "sfuser_real", "form_content");
        LOG.info("hear request:" + request);
        this.collector.emit(new Values(request));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cr"));

    }
}
