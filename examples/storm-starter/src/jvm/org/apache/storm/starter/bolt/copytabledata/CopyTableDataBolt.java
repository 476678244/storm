package org.apache.storm.starter.bolt.copytabledata;

import org.apache.log4j.Logger;
import org.apache.storm.starter.bean.CopyTableDataRequest;
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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        CopyTableDataRequest request = (CopyTableDataRequest) input.getValue(0);
        LOG.info("CopyTableDataBolt get request:" + request);
        this.collector.emit(new Values(request));
        LOG.info("CopyTableDataBolt emit request:" + request);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cr"));
    }
}
