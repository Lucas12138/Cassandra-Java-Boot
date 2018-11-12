package db.beans;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Database bean mapping to Cassandra's table, user_activity
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-09 12:32 AM
 */
public class UserActivityBean extends DBBean {

    private String pid;
    private String uid;
    private Date day;
    private double moneySpent;

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Date getDay() {
        return day;
    }

    public void setDay(Date day) {
        this.day = day;
    }

    public double getMoneySpent() {
        return moneySpent;
    }

    public void setMoneySpent(double moneySpent) {
        this.moneySpent = moneySpent;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return "UserActivityBean{" +
                "pid='" + pid + '\'' +
                ", uid='" + uid + '\'' +
                ", day=" + ((day == null) ? "null" : sdf.format(day)) +
                ", moneySpent=" + moneySpent +
                '}';
    }
}
