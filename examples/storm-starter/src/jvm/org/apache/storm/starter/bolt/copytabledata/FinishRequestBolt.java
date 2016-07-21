package org.apache.storm.starter.bolt.copytabledata;

import org.apache.log4j.Logger;
import org.apache.storm.starter.bean.CopyTableDataRequest;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by zonghan on 7/20/16.
 */
public class FinishRequestBolt extends BaseBasicBolt {

    private static final Logger LOG = Logger.getLogger(CopyTableDataBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        CopyTableDataRequest request = (CopyTableDataRequest) input.getValue(0);
        LOG.info("FinishRequestBolt finish request:" + request);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
