package db.daos;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.google.common.collect.Lists;

import db.config.CassandraConnector;
import db.beans.DBBean;
import utils.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Prepare queries on Cassandra table and use callback style to simplify the usage of this class
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-10 10:26 PM
 */
public class CqlRequest {

    // cache the prepared statements with concurrent hashmap
    private final static ConcurrentHashMap<String, PreparedStatement> PREPARED_STATEMENT_CACHE = new ConcurrentHashMap<>();
    // time to wait until query finishes, if not finished will retry after
    private final int timeout = 1000;
    // number of concurrent statements allowed
    private int nConcurrentStatements;
    private PreparedStatement preparedStatement;
    // a list of our bound statements to execute
    private List<BoundStatement> boundStatements = new ArrayList<>();
    // TODO: implement the streaming style query (because the returned data size can be very large)
    // streaming handler, if set we can stream back partial lists using lambda functions
    private HandlerRowList handler;
    // whether this query failed or not
    private boolean success = true;

    public boolean isFailed() {
        return !success;
    }

    /**
     * Construct an object to execute desired queries, with internal backoff and retry mechanisms in case of failures.
     * Also can execute multiple statements in parallel by using addStatements, all statements that are added
     * will be executed upon calling treat()
     * @param query query string with placeholders (?) that can be bound by calling addStatements()
     */
    public CqlRequest(final String query) {
        if (query.trim().toUpperCase().startsWith("SELECT")) {
            // not optimized for read
            nConcurrentStatements = 32; // if we fetch 10 statements, each 10k items(~max partition size), we could get upto 100k!
        } else {
            // optimized for write
            nConcurrentStatements = 10000; // we can write a lot more!
        }

        // prepare our query, computeIfAbsent is atomic on concurrentHashmap, if key does not exist yet.
        this.preparedStatement = PREPARED_STATEMENT_CACHE.computeIfAbsent(query, k -> CassandraConnector.getSession().prepare(k));
    }

    /**
     * Use this to override the number of concurrent statements setting (in case of read intensive scenarios)
     * @param query
     * @param nConcurrentStatements
     */
    public CqlRequest(final String query, int nConcurrentStatements) {
        this(query);
        this.nConcurrentStatements = nConcurrentStatements;
    }


    /**
     * Bind the parameters to the prepared statement and add the bound statement to the list
     * @param params
     */
    public void addStatements(Object... params) {
        BoundStatement queryBound = preparedStatement.bind(params);
        queryBound.setFetchSize(20000);
        queryBound.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        queryBound.setIdempotent(true);
        boundStatements.add(queryBound);
    }

    /**
     * fields should be in order of the constructed query
     * @param bean
     * @param fields
     */
    public void addStatementsAsBeans(DBBean bean, String... fields) {
        Map<String, Object> beanAsMap = bean.toCassandraObject();
        Object[] params = new Object[fields.length];
        for (int i =0; i<fields.length; i +=1) {
            params[i] = beanAsMap.get(fields[i].toLowerCase());
            if(params[i]==null) {
                Log.i("WARNING: bean object does not contain given field " + fields[i]);
            }
        }
        addStatements(params);
    }

    /**
     * Handler for callback
     */
    public interface HandlerRowList {
        void callback(List<Row> partialRows);
    }

    /**
     * Get query results
     * @return
     */
    public List<Row> treat() {
        if (this.boundStatements.isEmpty()) {
            Log.i("WARNING: No bound statements, either this query was already treated or no statements were added");
        }
        this.success = true;
        try {
            Session session = CassandraConnector.getSession();
            // insert/update/delete queries won't return result rows
            List<Row> res = executeQuery(session, this.boundStatements);
            // clear our statements as we assume calling this object twice with new statements is allowed
            this.boundStatements.clear();
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            Log.i(e.getMessage());
            this.success = false;
            return null;
        }
    }

    /**
     * TODO: use this method when implementing stream query
     * @param streamingHandler
     * @return
     */
    public boolean streamRows(HandlerRowList streamingHandler) {
        this.handler = streamingHandler;
        treat();
        return this.success;
    }

    /**
     * Execute query and get rows, backoff and retry if run into failures
     * @param session
     * @param statements
     * @return
     */
    private List<Row> executeQuery(Session session, List<BoundStatement> statements) {
        List<Row> collectedRows = new ArrayList<>();
        int attemptCounter = 1;
        this.success = true;

        List<BoundStatement> failedStatements = executeQueryByBuffer(session, statements, collectedRows);
        // if some failed, we need to retry them, failing once is common, twice would indicate small issues, more probably severe issues
        while (failedStatements.size() > 0 && attemptCounter <= 15) {
            attemptCounter += 1;
            // delay by fixed time + random extra time (random exponential backoff), upto 2^10+2^10*25 = ~ 1min
            double expFactor = Math.pow(2, attemptCounter-1);
            int baseDelay = 25;
            if (attemptCounter>2) {
                long backoffTime = (long)((expFactor*baseDelay)+(Math.random()*baseDelay*expFactor));
                Log.i("Remaining requests: " + Integer.toString(failedStatements.size()) +
                        " Attempt: " + Integer.toString(attemptCounter) +
                        " Backing-off: " + Long.toString(backoffTime) + "ms");
                try {Thread.sleep(backoffTime);} catch (InterruptedException e) {e.printStackTrace();}
            }
            failedStatements = executeQueryByBuffer(session, failedStatements, collectedRows);
        }

        if (failedStatements.size()>0) {
            Log.e("ERROR: Could not completely execute all requests: " + Integer.toString(failedStatements.size()) +" for query: " + this.preparedStatement.getQueryString(), null);
            this.success = false;
        }
        return collectedRows;
    }


    /**
     * Execute query in a batch style
     * @param session
     * @param statements
     * @param collectedRows
     * @return
     */
    private List<BoundStatement> executeQueryByBuffer(Session session, List<BoundStatement> statements, List<Row> collectedRows) {
        List<BoundStatement> failedStatements = new ArrayList<>();

        // do it in batch, to avoid overloading our threads and network, this is linked to setMaxRequestsPerConnection
        for (List<BoundStatement> partition : Lists.partition(statements, nConcurrentStatements)) {
            // use async to get a list of resultsets
            List<ResultSetWithStatement> resultSets = partition.stream()
                    .map(statement -> new ResultSetWithStatement(session.executeAsync(statement), statement))
                    .collect(Collectors.toList());

            // collect resulting rows from the future result sets
            List<Row> collectRowsPartial = resultSets.stream().map(futurePair -> {
                ResultSet result = null;
                ResultSetFuture future = futurePair.resultSetFuture;
                BoundStatement queryBound = futurePair.statement;
                try {
                    result = future.get(timeout, TimeUnit.MILLISECONDS);
                    return result.all();
                } catch (InterruptedException | ExecutionException | TimeoutException | OperationTimedOutException | ReadTimeoutException e) {
                    e.printStackTrace();
                } catch (Exception e){Log.i(e.getMessage());}

                // add failed statements
                failedStatements.add(queryBound);
                return null;
            }).filter(a -> a != null).flatMap(a -> a.stream()).collect(Collectors.toList()); // filter and merge each partialRowList

            if(collectRowsPartial.size()>500000) {
                Log.w("WARNING: nConcurrentStatements too large, or partition size too large: "+Integer.toString(collectRowsPartial.size())+" rows, for query: " + this.preparedStatement.getQueryString(), null);
            }

            // check if it's using callback style
            if (this.handler != null) {
                // use callback
                this.handler.callback(collectRowsPartial);
            } else {
                // no callback, we simply return the full row set
                collectedRows.addAll(collectRowsPartial);
            }
        }
        return failedStatements;
    }

    /**
     * Combine ResultSetFuture with BoundStatement to a new result type class
     * this can help redo the query in case of failure
     */
    class ResultSetWithStatement {
        public ResultSetFuture resultSetFuture;
        public BoundStatement statement;
        public ResultSetWithStatement(ResultSetFuture resultSetFuture, BoundStatement statement) {
            this.resultSetFuture = resultSetFuture;
            this.statement = statement;
        }
    }
}
