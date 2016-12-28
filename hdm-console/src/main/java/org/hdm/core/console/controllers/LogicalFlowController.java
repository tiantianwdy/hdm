package org.hdm.core.console.controllers;

import org.hdm.akka.server.SmsSystem;
import org.hdm.core.console.models.DagGraph;
import org.hdm.core.console.models.DagGraph;
import org.hdm.core.console.views.HDMViewAdapter;
import org.hdm.core.message.LogicalFLowQuery;
import org.hdm.core.message.LogicalFLowResp;
import scala.Option;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 8/04/16.
 */
public class LogicalFlowController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String exeId = req.getParameter("executionTag");
        Boolean opt = Boolean.parseBoolean(req.getParameter("opt"));
        LogicalFLowQuery msg = new LogicalFLowQuery(exeId, opt);
        String actor = AbstractController.masterExecutor;
        Option objRes = SmsSystem.askSync(actor, msg);
        if(objRes != null) {
            LogicalFLowResp res = (LogicalFLowResp) objRes.get();
            DagGraph vo = HDMViewAdapter.HDMPojoSeqToGraph(res.results());
            String json = ObjectUtils.objectToJson(vo);
            resp.setContentType("application/json");
            resp.getWriter().write(json);
        } else {
            System.err.println("Obtain null response from HDM server.");
            String json = "Msg:Error";
            resp.setContentType("application/json");
            resp.getWriter().write(json);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }
}
