package db.config;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import utils.Log;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Create a Cassandra connection with singleton pattern
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-08 1:29 AM
 */
public class CassandraConnector {

    /**
     * Singleton object to connect to database (only can be connected to single keyspace at a time)
     */
    private static CassandraConnector connection;
    private final Session session;
    private final Cluster cluster;
    private final String clusterName;
    private final String keyspace;
    private final String ipAddress;
    private final int port;
    private final String username;
    private final String password;

    //TODO: consider using different consistency levels for different kinds of operations
    private CassandraConnector(String clusterName, String keyspace, String ipAddress, int port, String username, String password) {
        this.clusterName = clusterName;
        this.keyspace = keyspace;
        this.ipAddress = ipAddress;
        this.port = port;
        this.username = username;
        this.password = password;
        this.cluster = initCluster(username, password);
        this.session = cluster.connect(this.keyspace);
    }

    public static String getKeyspace() {
        return connection.keyspace;
    }

    public static String getIpAddress() {
        return connection.ipAddress;
    }

    public static int getPort() {
        return connection.port;
    }

    public static String getUsername() {
        return connection.username;
    }

    public static String getPassword() {
        return connection.password;
    }

    /**
     * Get cassandra connection string
     * @return
     */
    public static String getConnectionInfoString() {
        if (connection==null) {
            return "";
        }
        return connection.clusterName + "://" + connection.ipAddress + ":" + Integer.toString(connection.port)+ "/" + connection.keyspace + "?" +"user="+ connection.username;
    }

    /**
     * Singleton pattern, only a single shared connection object
     * @param clusterName
     * @param keyspace
     * @param ipAddress
     * @param port
     * @param username
     * @param password
     */
    public static void initializeConnection(String clusterName, String keyspace, String ipAddress, int port, String username, String password) {
        connection = new CassandraConnector(clusterName, keyspace, ipAddress, port, username, password);
    }

    /**
     * Method to initialize th cluster
     * @return
     */
    public static boolean initialize() {
        try {
            //TODO: maybe use config files, env variables or <init-param> in web.xml for credentials
            String keySpace = "test_db";
            String clusterName = "localhost";
            String ip = "127.0.0.1";
            int port = 9042;
            String userName = "cassandra";
            String password = "cassandra";
            CassandraConnector.initializeConnection(clusterName, keySpace, ip, port, userName, password);
            Log.i("Cassandra connection: OK  " + clusterName);
        }
        catch( Error | Exception e ) {
            Log.e( "Error initializing Cassandra!", e);
            return false;
        }
        return true;
    }

    /**
     * Gets the shared session object, which is thread safe, all requests are executed using this session
     * @return
     */
    public static Session getSession(){
        if(connection==null || connection.session.isClosed() || connection.cluster.isClosed()) {
            throw new RuntimeException("Database not initialized or connection has been closed");
        }
        return connection.session;
    }

    /**
     * Initialize cluster with some settings like connection pool, ip whitelist etc.
     * @param username
     * @param password
     * @return
     */
    private Cluster initCluster(String username, String password) {
        // Config connection pool
        PoolingOptions poolingOptions = new PoolingOptions()
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768) //limit: 32768
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2048)
                .setMaxQueueSize(100000)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 8)
                .setPoolTimeoutMillis(0);

        // Config ip whitelist
        List<InetSocketAddress> whiteList= new ArrayList<>();
        whiteList.add(new InetSocketAddress(ipAddress, port));

        // Config cluster
        Cluster cluster = Cluster.builder()
                .withClusterName(clusterName)
                .withPort(port)
                .addContactPoint(ipAddress)
                .withPoolingOptions(poolingOptions)
                .withLoadBalancingPolicy(new WhiteListPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()), whiteList))
                .withAuthProvider(new PlainTextAuthProvider(username, password))
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 10 * 60000)) // maybe useful in case nodes are down
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .build();
        return cluster;
    }

    /**
     * Given a keyspace name and a table name, check if it exists
     * @param keyspaceName
     * @param tableName
     * @return
     */
    public static boolean checkTableIfExists(String keyspaceName, String tableName) {
        KeyspaceMetadata keyspace = connection.cluster.getMetadata().getKeyspace(keyspaceName);
        if (keyspace == null) {
            Log.i("The given keyspace -> " + keyspaceName + " doesn't exist!");
            return false;
        }
        TableMetadata table = keyspace.getTable(tableName);
        if (table == null) {
            Log.i("The given table -> " + tableName + " doesn't exist!");
            return false;
        }
        return true;
    }

    /**
     * Closes the session and cluster connection.
     */
    public static boolean shutdown() {
        connection.session.close();
        connection.cluster.close();
        boolean closed = connection.session.isClosed() && connection.cluster.isClosed();
        connection = null;
        return closed;
    }
}