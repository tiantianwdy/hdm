package org.hdm.core.console.controllers;

import org.hdm.akka.server.SmsSystem;
import org.hdm.core.console.models.TreeVO;
import org.hdm.core.console.models.TreeVO;
import org.hdm.core.console.views.HDMViewAdapter;
import org.hdm.core.message.AllAppVersionsQuery;
import org.hdm.core.message.AllAppVersionsResp;
import org.hdm.core.message.DependencyTraceQuery;
import org.hdm.core.message.DependencyTraceResp;

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
