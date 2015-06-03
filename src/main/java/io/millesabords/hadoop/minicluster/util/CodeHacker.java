package io.millesabords.hadoop.minicluster.util;

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
 * Class replacing some methods of Hadoop by other ones.
 *
 * @author Bruno Bonnin
 *
 */
public class CodeHacker {
    
    private static final ClassPool classPool = ClassPool.getDefault();
    
    /**
     * Replace a method by another one.
     * 
     * @param className Complete name of the class
     * @param methodName Name of the method
     * @param newSrc New code of the method
     * @param invokeToClass Force or not invocation of toClass() method to take into account update
     * @throws NotFoundException 
     * @throws CannotCompileException 
     */
    private static void replaceMethod(String className, String methodName, String newSrc, boolean invokeToClass) throws NotFoundException, CannotCompileException {
        
        final CtClass ctClass = classPool.get(className);
        
        final CtMethod oldMethod = ctClass.getDeclaredMethod(methodName);
        ctClass.removeMethod(oldMethod);
        
        final CtMethod newMethod = CtNewMethod.make(newSrc, ctClass);
        ctClass.addMethod(newMethod);
        
        if (invokeToClass) {
            ctClass.toClass();
        }
        
    }
    
    /**
     * Replace a field by another one.
     * 
     * @param className Complete name of the class
     * @param fieldName Name of the field
     * @param newSrc New source of the field
     * @param invokeToClass Force or not invocation of toClass() method to take into account update
     * @throws NotFoundException 
     * @throws CannotCompileException 
     */
    private static void replaceField(String className, String fieldName, String newSrc, boolean invokeToClass) throws NotFoundException, CannotCompileException {
        
        final CtClass ctClass = classPool.get(className);
        
        final CtField oldField = ctClass.getDeclaredField(fieldName);
        ctClass.removeField(oldField);
        
        final CtField newField = CtField.make(newSrc, ctClass);
        ctClass.addField(newField);
        
        if (invokeToClass) {
            ctClass.toClass();
        }
        
    }
    
    /**
     * Insert code before the method.
     * 
     * @param className Complete name of the class
     * @param methodName Name of the method
     * @param code New code to insert in the method
     * @param invokeToClass Force or not invocation of toClass() method to take into account update
     * @throws NotFoundException
     * @throws CannotCompileException
     */
    private static void insertBefore(String className, String methodName, String code, boolean invokeToClass) throws NotFoundException, CannotCompileException {
        
        final CtClass ctClass = classPool.get(className);
        
        final CtMethod ctMethod = ctClass.getDeclaredMethod(methodName);
        ctMethod.insertBefore(code);
        
        if (invokeToClass) {
            ctClass.toClass();
        }
    }

    /**
     * Initialisation of the new code.
     *
     * @throws Exception 
     */
    public static void init() throws Exception {
                
        if (System.getProperty("os.name").startsWith("Windows")) {
//            System.setProperty("os.name", "Linux"); // Does not work, because it implies the use of unix specific Java classes            
       
            replaceMethod("org.apache.hadoop.io.nativeio.NativeIO$Windows", "access0",  
                    "private static boolean access0(String path, int requestedAccess) {" +
                    "    return true;" +
                    "}", true);
            
            replaceMethod("org.apache.hadoop.util.Shell", "getWinUtilsPath", 
                    "public static final String getWinUtilsPath() {" +
                    "    return System.getProperty(\"user.dir\") + \"\\\\winutils.bat\";" +
                    "}", false);   
            
            replaceMethod("org.apache.hadoop.util.Shell", "getGetPermissionCommand",
                    "public static String[] getGetPermissionCommand() {" +
                    "    return (WINDOWS) ? new String[] { WINUTILS, \"perm\" }" +
                    "       : new String[] { \"/bin/ls\", \"-ld\" };" +
                    "}", false);
            
            insertBefore("org.apache.hadoop.fs.RawLocalFileSystem$DeprecatedRawLocalFileStatus", "loadPermissionInfo", 
                    "{ org.apache.hadoop.util.Shell.TOKEN_SEPARATOR_REGEX = \"[ \\\t\\\n\\\r\\\f]\"; }", true);
            
            // Just for removing "static" modifier of this field
            replaceField("org.apache.hadoop.util.Shell", "TOKEN_SEPARATOR_REGEX", 
                    "public static String TOKEN_SEPARATOR_REGEX; = \"\";", true);
            
        }
    }
}
