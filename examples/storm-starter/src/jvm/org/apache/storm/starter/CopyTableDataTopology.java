package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.bean.CopyTableDataRequest;
import org.apache.storm.starter.bean.RequestStatusEnum;
import org.apache.storm.starter.bolt.copytabledata.CopyTableDataBolt;
import org.apache.storm.starter.bolt.copytabledata.FinishRequestBolt;
import org.apache.storm.starter.mongodb.CopyDataRequestDAO;
import org.apache.storm.starter.mongodb.MorphiaSingleton;
import org.apache.storm.starter.spout.copytabledata.RequestListenSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.mongodb.morphia.Datastore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zonghan on 7/20/16.
 */
public class CopyTableDataTopology {
    public static void main(String[] args) throws Exception {
        // test data
        Datastore ds = MorphiaSingleton.getDatastore();
        ds.delete(ds.createQuery(CopyTableDataRequest.class));
        List<CopyTableDataRequest> requests = new ArrayList<>();
        CopyTableDataRequest request = new CopyTableDataRequest(
                "jdbc:oracle:thin:@10.58.100.66:1521:dbpool1", "sfuser", "sfuser", "sfuser_tree", "rbp_perm_role")
                .setTargetConnectionUrl("jdbc:oracle:thin:@10.58.100.66:1521:dbpool1").setTargetSchema("sfuser")
                .setTargetPassword("sfuser").setTargetSchema("sfuser_temp2").setTargetUsername("sfuser")
                .setIdColumnName("role_id").setStartId(1).setEndId(500);
        requests.add(request);
        requests.add(((CopyTableDataRequest) request.clone()).setStartId(501).setEndId(1000));
        requests.add(((CopyTableDataRequest) request.clone()).setTable("rbp_perm_rule")
                .setIdColumnName("rule_id").setStartId(1).setEndId(500));
        requests.stream().forEach(r -> {
            ds.save(r);
        });

        // builder
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("hear_request", new RequestListenSpout(), 1);
        builder.setBolt("process", new CopyTableDataBolt(), 1).shuffleGrouping("hear_request");
        builder.setBolt("finish", new FinishRequestBolt(), 1).shuffleGrouping("process");

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("CopyTableDataTopology", conf, builder.createTopology());
            //Utils.sleep(20000);
            while (!requests.isEmpty()) {
                requests = ds.find(CopyTableDataRequest.class).asList();
                Utils.sleep(100);
                final Iterator<CopyTableDataRequest> requestIterator = requests.iterator();
                while (requestIterator.hasNext()) {
                    CopyTableDataRequest r = requestIterator.next();
                    if (r.getStatus() == RequestStatusEnum.FINISHED) {
                        requestIterator.remove();
                    }
                }
            }
            cluster.killTopology("CopyTableDataTopology");
            cluster.shutdown();
        }
    }
}
