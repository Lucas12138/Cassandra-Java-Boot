package db;

import db.config.CassandraConnector;
import db.daos.UserActivityDAO;

/**
 * A model will take care of database connection and aggregate DAOs
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-11 6:04 PM
 */
public class Model {

    private UserActivityDAO uaDAO;

    public Model() {
        CassandraConnector.initialize();
        uaDAO = new UserActivityDAO();
    }

    public UserActivityDAO getUaDAO() {
        return uaDAO;
    }
}
