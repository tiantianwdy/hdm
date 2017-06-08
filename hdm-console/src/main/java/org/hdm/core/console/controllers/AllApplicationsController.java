package org.hdm.core.console.controllers;

import org.hdm.core.console.models.TreeVO;
import org.hdm.core.console.views.HDMViewAdapter;
import org.hdm.core.message.AllApplicationsQuery;
import org.hdm.core.message.AllApplicationsResp;
import org.hdm.akka.server.SmsSystem;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 25/02/17.
 */
public class AllApplicationsController extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        AllApplicationsQuery msg = new AllApplicationsQuery();
        String actor = AbstractController.masterExecutor;
        AllApplicationsResp res = (AllApplicationsResp) SmsSystem.askSync(actor, msg).get();
        TreeVO vo = HDMViewAdapter.allApplicationsRespToTreeVO(res);
        String json = ObjectUtils.objectToJson(vo);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req, resp);
    }
}
