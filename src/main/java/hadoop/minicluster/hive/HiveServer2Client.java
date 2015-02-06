package hadoop.minicluster.hive;

import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCLIService.Client;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 
 * Class for starting a client of the HiveServer2.
 *
 * @author Bruno Bonnin
 *
 */
public class HiveServer2Client {
    
    private static final Logger logger = LoggerFactory.getLogger(HiveServer2Client.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 11100;
    
    private Client client;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    
    public HiveServer2Client(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public void startTests() {
        try {
            initClient(createBinaryTransport());
            createTable();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private TTransport createBinaryTransport() throws Exception {
        final TTransport transport = PlainSaslHelper.getPlainTransport("anonymous", "anonymous", new TSocket(host, port));
        transport.open();
        return transport;
    }

    private void initClient(TTransport transport) {
        final TProtocol protocol = new TBinaryProtocol(transport);
        client = new TCLIService.Client(protocol);
    }

    private TExecuteStatementResp executeQuerySync(String queryString, TSessionHandle sessHandle) throws Exception {
        
        final TExecuteStatementReq execReq = new TExecuteStatementReq();
        execReq.setSessionHandle(sessHandle);
        execReq.setStatement(queryString);
        execReq.setRunAsync(false);
        
        final TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
        
        return execResp;
    }

    private void createTable() throws Exception {
        
        final TOpenSessionReq openReq = new TOpenSessionReq();
        final TSessionHandle sessHandle = client.OpenSession(openReq).getSessionHandle();
        
        String queryString = "SET hive.lock.manager=org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
        executeQuerySync(queryString, sessHandle);
        
        executeQuerySync("CREATE DATABASE IF NOT EXISTS es", sessHandle);
        executeQuerySync("USE es", sessHandle);
        
        queryString = "DROP TABLE IF EXISTS siebel_adresse";
        executeQuerySync(queryString, sessHandle);

        queryString = "CREATE EXTERNAL TABLE IF NOT EXISTS siebel_adresse (" +
                        "a_addr STRING, a_addr_line_2 STRING, a_addr_line_3 STRING, a_addr_line_4 STRING, " +
                        "a_addr_line_5 STRING, a_city STRING, a_country STRING, a_last_upd TIMESTAMP, " +
                        "a_principal_flg STRING, a_province STRING, a_row_id STRING, a_x_uge_flg STRING, " +
                        "a_zipcode STRING, b_active_flg STRING, d_asset_num STRING) " +
                        "STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' " +
                        "TBLPROPERTIES('es.resource'='siebel/adresse', 'es.index.auto.create'='false', 'es.nodes'='192.168.1.45')";
        executeQuerySync(queryString, sessHandle);
        
        final TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
        client.CloseSession(closeReq);
    }
    
    /**
     * Main.
     * 
     * @param args Command line arguments
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        
        final AbstractApplicationContext ctx = new ClassPathXmlApplicationContext("hive-server2-client.xml");
        ctx.registerShutdownHook();

        try {
            final HiveServer2Client client = (HiveServer2Client) ctx.getBean(HiveServer2Client.class);
            
           client.startTests();
        }
        finally {
            ctx.close();
        }
    }
}
