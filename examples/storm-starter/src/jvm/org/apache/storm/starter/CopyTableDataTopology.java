package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.bolt.copytabledata.CopyTableDataBolt;
import org.apache.storm.starter.bolt.copytabledata.FinishRequestBolt;
import org.apache.storm.starter.spout.copytabledata.RequestListenSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by zonghan on 7/20/16.
 */
public class CopyTableDataTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("request", new RequestListenSpout(), 1);
        builder.setBolt("process", new CopyTableDataBolt(), 1).shuffleGrouping("request");
        builder.setBolt("finish", new FinishRequestBolt(), 1).shuffleGrouping("process");

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("CopyTableDataTopology", conf, builder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology("CopyTableDataTopology");
            cluster.shutdown();
        }
    }
}
