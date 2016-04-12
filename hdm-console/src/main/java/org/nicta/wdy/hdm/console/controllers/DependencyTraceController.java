package org.nicta.wdy.hdm.console.controllers;

import com.baidu.bpit.akka.server.SmsSystem;
import org.nicta.wdy.hdm.console.models.TreeVO;
import org.nicta.wdy.hdm.console.views.HDMViewAdapter;
import org.nicta.wdy.hdm.message.AllAppVersionsQuery;
import org.nicta.wdy.hdm.message.AllAppVersionsResp;
import org.nicta.wdy.hdm.message.DependencyTraceQuery;
import org.nicta.wdy.hdm.message.DependencyTraceResp;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 12/04/16.
 */
public class DependencyTraceController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String app = req.getParameter("app");
        String version = req.getParameter("version");
        DependencyTraceQuery msg = new DependencyTraceQuery(app, version);
        String actor = AbstractController.masterExecutor;
        DependencyTraceResp res = (DependencyTraceResp) SmsSystem.askSync(actor, msg).get();
        TreeVO vo = HDMViewAdapter.dependencyTraceToTreeVO(res);
        String json = ObjectUtils.objectToJson(vo);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }
}
