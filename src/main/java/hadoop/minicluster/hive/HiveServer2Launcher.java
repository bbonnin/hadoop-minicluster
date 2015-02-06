package hadoop.minicluster.hive;

import org.apache.hadoop.conf.Configuration;
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

    private static final int DEFAULT_PORT = 11100;
    
    private HiveServer2 server;
    private HiveConf config;
    private String host;
    private int port = DEFAULT_PORT;
    
    public HiveServer2Launcher() {
    }
    
    public void setHost(String host) {
        this.host = host;
        if (this.config != null && this.host !=  null) {
            this.config.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
        }
    }

    public void setPort(int port) {
        this.port = port;
        if (this.config != null) {
            this.config.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
        }
    }

    public void setConfig(Configuration config) {
        this.config = new HiveConf(config, config.getClass());
        this.config.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
        if (this.host != null) {
            this.config.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
        }
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
}
