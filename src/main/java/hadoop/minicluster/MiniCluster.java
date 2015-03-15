package hadoop.minicluster;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

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
public class MiniCluster {

    private static final Logger logger = LoggerFactory.getLogger(MiniCluster.class);
    
    /**
     * Replace a method by another one.
     * 
     * @param className Complete name of the class
     * @param methodName Name of the method
     * @param newSrc New code of the method
     * @throws NotFoundException 
     * @throws CannotCompileException 
     */
    private static void replaceMethod(String className, String methodName, String newSrc) throws NotFoundException, CannotCompileException {
        
        final ClassPool classPool = ClassPool.getDefault();
        final CtClass ctClass = classPool.get(className);
        
        final CtMethod oldMethod = ctClass.getDeclaredMethod(methodName);
        ctClass.removeMethod(oldMethod);
        
        final CtMethod newMethod = CtNewMethod.make(newSrc, ctClass);
        ctClass.addMethod(newMethod);
        ctClass.toClass();
        
    }

    /**
     * Main.
     * 
     * @param args Command line arguments
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        
        if (System.getProperty("os.name").startsWith("Windows")) {
//            System.setProperty("os.name", "Linux"); // Does not work, because it implies the use of unix specific classes
//            System.setProperty("HADOOP_USER_NAME", System.getProperty("user.name"));
//            System.setProperty ("file.separator", "/");
//            System.setProperty("user.dir", System.getProperty("user.dir").replace("\\", "/").replace("C:", "/cygdrive/c"));
//            System.setProperty("java.security.auth.login.config", "jaas.config");
            
//            replaceMethod("org.apache.hadoop.security.UserGroupInformation", "getOSLoginModuleName",
//                    "private static String getOSLoginModuleName() {" +
//                    "    return \"com.sun.security.auth.module.NTLoginModule\";" +
//                    "}");
            
            replaceMethod("org.apache.hadoop.io.nativeio.NativeIO$Windows", "access0",  
                    "private static boolean access0(String path, int requestedAccess) {" +
                    "    System.out.println(\"access0 : path=\" + path + \", access=\" + requestedAccess);" +
                    "    return true;" +
                    "}");
            
            replaceMethod("org.apache.hadoop.util.Shell", "getWinUtilsPath", 
                    "public static final String getWinUtilsPath() {" +
                    "    return System.getProperty(\"user.dir\") + \"\\\\winutils.bat\";" +
                    "}");            
        }

        logger.info("Starting cluster...");

        AbstractApplicationContext ctx = null;
        
        try {
            ctx = new ClassPathXmlApplicationContext("hadoop-minicluster.xml");
            ctx.registerShutdownHook();
            
            logger.info("Cluster started !");
            
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
