package db.daos;

import db.beans.UserActivityBean;

/**
 * A DAO for user_activity table
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-10 11:12 PM
 */
public class UserActivityDAO<T extends UserActivityBean> extends GenericDAO<T> {

    /**
     * A singleton dao object to access RootMetricDAO base functions or overriden/extended UserActivityCRUDCassandra functions.
     */
    public static final UserActivityDAO<UserActivityBean> dao = new UserActivityDAO<>();

    public UserActivityDAO() {
        super("user_activity", (Class<T>) UserActivityBean.class);
    }

    // TODO: implement a stream style query here
}
