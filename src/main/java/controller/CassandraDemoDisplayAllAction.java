package controller;

import db.Model;
import db.beans.UserActivityBean;
import db.daos.UserActivityDAO;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.*;

/**
 * Cassandra Demo Action: display all
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-11 5:41 PM
 */
public class CassandraDemoDisplayAllAction extends Action{
    private static final long serialVersionUID = 1L;
    private UserActivityDAO uaDAO;
    public CassandraDemoDisplayAllAction(Model model) {
        uaDAO = model.getUaDAO();
    }

    @Override
    public String getName() {
        return "demo.do";
    }

    @Override
    public String performGet(HttpServletRequest request) {
        List<String> errors = new ArrayList<String>();
        request.setAttribute("errors", errors);

        try {
            List<UserActivityBean> uaBeans = uaDAO.select();
            // sort by day
            Collections.sort(uaBeans, (b1, b2) -> b1.getDay().compareTo(b2.getDay()));
            request.setAttribute("beans", uaBeans);
            return "demo.jsp";

        } catch (Exception e) {
            errors.add(e.getMessage());
            return "error.jsp";
        }
    }

    @Override
    public String performPost(HttpServletRequest request) {
        return performGet(request);
    }
}
