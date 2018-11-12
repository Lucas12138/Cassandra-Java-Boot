package utils;


import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Utils for logging
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-08 1:39 AM
 */
public class Log {

    private static Logger log;
    private static Level logLevel;

    public static Logger getLog() {
        if (log == null) {
            log = LogManager.getLogger(Log.class.getName());
        }
        return log;
    }

    private static Level getLevelFromString(String level) {
        switch (level.toUpperCase()) {
            case "DEBUG": return Level.DEBUG;
            case "INFO":  return Level.INFO;
            case "WARN":  return Level.WARN;
            case "ERROR": return Level.ERROR;
            default:      return Level.DEBUG;
        }
    }

    public static void initialize(String level) {
        // set log level
        logLevel = getLevelFromString(level);
        getLog().setLevel(logLevel);
        System.setProperty("log_level", logLevel.toString());

        // set log file path
        System.setProperty("log_path", System.getProperty("user.home") +"/application_log");

        // config based on properties file
        PropertyConfigurator.configure("resource/config/log.properties");
    }

    // DEBUG
    public static void d(String message) {
        getLog().debug(message);
    }
    public static void d(String message, Throwable t) {
        getLog().debug(message, t);
    }

    // INFO
    public static void i(String message) {
        getLog().info(message);
    }
    public static void i(String message, Throwable t) {
        getLog().info(message, t);
    }

    // WARN
    public static void w(String message) {
        getLog().warn(message);
    }
    public static void w(String message, Throwable t) {
        getLog().warn(message, t);
    }

    // ERROR
    public static void e(String message) {
        getLog().error(message);
    }
    public static void e(String message, Throwable t) {
        getLog().error(message, t);
    }

    // FATAL
    public static void f(String message) {
        getLog().fatal(message);
    }
    public static void f(String message, Throwable t) {
        getLog().fatal(message, t);
    }
}