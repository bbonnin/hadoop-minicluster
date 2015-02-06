package hadoop.minicluster.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.apache.hive.service.server.HiveServer2;

/**
 * 
 * Class for starting a Hive Server2.
 *
 * @author Bruno Bonnin
 *
 */
public class HiveServer2Launcher {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 11100;
    
    private HiveServer2 server;
    private HiveConf config;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    
    public HiveServer2Launcher(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public void setConfig(HiveConf config) {
        this.config = config;
        this.config.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
        this.config.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
        this.config.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
        this.config.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NOSASL.toString());
        this.config.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");
    }
    
    public void start() {
        server = new HiveServer2();
        server.init(config);
        server.start();
    }
    
    public void stop() {
        if (server != null) {
            server.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        hiveConf.set("hive.metastore.warehouse.dir", "file:///tmp");
        //hiveConf.set("hive.server2.thrift.port", "11100");
        hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
        hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, DEFAULT_HOST);
        hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, DEFAULT_PORT);
        hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NOSASL.toString());
        hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");

        /*<!--hive.metastore.local=true
                mapreduce.framework.name=yarn
                hive.exec.submitviachild=false-->
                hive.debug.localtask=true
                hive.auto.convert.join.use.nonstaged=true*/
        HiveServer2 server = new HiveServer2();
        server.init(hiveConf);
        server.start();

        //initClient(createBinaryTransport());
    }
}
