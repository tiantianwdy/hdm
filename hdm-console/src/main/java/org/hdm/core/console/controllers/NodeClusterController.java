package org.hdm.core.console.controllers;

import org.hdm.akka.server.SmsSystem;
import org.hdm.core.console.models.DagGraph;
import org.hdm.core.console.views.HDMViewAdapter;
import org.hdm.core.messages.AllSLavesResp;
import org.hdm.core.messages.AllSlavesQuery;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 11/04/16.
 */
public class NodeClusterController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        AllSlavesQuery msg = new AllSlavesQuery(AbstractController.master);
        String actor = AbstractController.masterExecutor;
        AllSLavesResp res = (AllSLavesResp) SmsSystem.askSync(actor, msg).get();
        DagGraph vo = HDMViewAdapter.slaveClusterToGraphVO(res.results());
        String json = ObjectUtils.objectToJson(vo);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }
}
