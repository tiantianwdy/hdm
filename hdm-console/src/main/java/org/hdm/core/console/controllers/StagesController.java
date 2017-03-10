package org.hdm.core.console.controllers;

import org.hdm.core.console.models.DagGraph;
import org.hdm.core.console.views.HDMViewAdapter;
import org.hdm.core.message.JobStageResp;
import org.hdm.core.message.JobStagesQuery;

import org.hdm.akka.server.SmsSystem;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 25/02/17.
 */
public class StagesController extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String app = req.getParameter("app");
        String version = req.getParameter("version");
        JobStagesQuery msg = new JobStagesQuery (app, version);
        String actor = AbstractController.masterExecutor;
        JobStageResp res = (JobStageResp) SmsSystem.askSync(actor, msg).get();
        DagGraph vo = HDMViewAdapter.StagesToGraph(res);
        String json = ObjectUtils.objectToJson(vo);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req, resp);
    }
}
