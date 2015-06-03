package io.millesabords.hadoop.minicluster;

import io.millesabords.hadoop.minicluster.util.CodeHacker;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * 
 * Main class starting a minicluster with Hadoop and Hive components.
 *
 * @author Bruno Bonnin
 *
 */
public class SimpleMiniCluster {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMiniCluster.class);
        
    
    /**
     * Main.
     * 
     * @param args Command line arguments
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
                
        CodeHacker.init();

        logger.info("Starting cluster...");

        AbstractApplicationContext ctx = null;
        
        try {
            ctx = new ClassPathXmlApplicationContext("hadoop-minicluster.xml");
            ctx.registerShutdownHook();
            
            logger.info("Cluster started !");
            
            final Configuration cfg = (Configuration) ctx.getBean("hadoopConfiguredConfiguration");
            System.out.println("Namenode : http://" + cfg.get("dfs.namenode.http-address") + "/webapps/hdfs/dfshealth.html");
            System.out.println("YARN Resource manager : http://" + cfg.get("yarn.resourcemanager.webapp.address"));
            System.out.println("Job History : http://" + cfg.get("mapreduce.jobhistory.webapp.address"));
            
            // Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaargh ! Shame on me !!!
            while (true) { 
                Thread.sleep(100000);
            }
        }
        finally {
            if (ctx != null) {
                ctx.close();
            }
            logger.info("Cluster stopped !");
        }
    }
    
}
