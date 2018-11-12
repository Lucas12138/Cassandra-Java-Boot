package controller;

import db.Model;
import db.beans.UserActivityBean;
import db.daos.UserActivityDAO;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * Cassandra Demo Action: delete
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-11 7:48 PM
 */
public class CassandraDemoDeleteAction extends Action{
    private static final long serialVersionUID = 1L;
    private UserActivityDAO uaDAO;
    public CassandraDemoDeleteAction(Model model) {
        uaDAO = model.getUaDAO();
    }

    @Override
    public String getName() {
        return "delete.do";
    }

    @Override
    public String performGet(HttpServletRequest request) {
        List<String> errors = new ArrayList<String>();
        request.setAttribute("errors", errors);

        try {
            return "delete.jsp";
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

            UserActivityBean uaBean = new UserActivityBean();
            uaBean.setPid(pid);
            uaBean.setUid(uid);
            uaDAO.delete(uaBean, new String[]{"pid", "uid"});
            return "delete.jsp";
        } catch (Exception e) {
            errors.add(e.getMessage());
            return "error.jsp";
        }
    }
}
