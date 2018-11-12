package controller;

import db.Model;
import db.beans.UserActivityBean;
import db.daos.UserActivityDAO;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Cassandra Demo Action: add
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-11 7:11 PM
 */
public class CassandraDemoAddAction extends Action{
    private static final long serialVersionUID = 1L;
    private UserActivityDAO uaDAO;
    public CassandraDemoAddAction(Model model) {
        uaDAO = model.getUaDAO();
    }

    @Override
    public String getName() {
        return "add.do";
    }

    @Override
    public String performGet(HttpServletRequest request) {
        List<String> errors = new ArrayList<String>();
        request.setAttribute("errors", errors);

        try {
            return "add.jsp";

        } catch (Exception e) {
            errors.add(e.getMessage());
            return "error.jsp";
        }
    }

    @Override
    public String performPost(HttpServletRequest request) {
        List<String> errors = new ArrayList<String>();
        request.setAttribute("errors", errors);

        try {
            String pid = request.getParameter("pid");
            String uid = request.getParameter("uid");
            String moneySpent = request.getParameter("moneySpent");

            UserActivityBean uaBean = new UserActivityBean();
            uaBean.setPid(pid);
            uaBean.setUid(uid);
            uaBean.setDay(new Date());
            uaBean.setMoneySpent(Double.parseDouble(moneySpent));

            uaDAO.upsert(uaBean, "pid", "uid", "day", "moneySpent");
            return "add.jsp";

        } catch (Exception e) {
            errors.add(e.getMessage());
            return "error.jsp";
        }
    }
}
