package org.nicta.wdy.hdm.console.controllers;

import com.baidu.bpit.akka.server.SmsSystem;
import org.nicta.wdy.hdm.console.models.DagGraph;
import org.nicta.wdy.hdm.console.views.HDMViewAdapter;
import org.nicta.wdy.hdm.message.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 8/04/16.
 */
public class PhysicalFlowController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String exeId = req.getParameter("executionTag");
        PhysicalFlow msg = new PhysicalFlow(exeId);
        String actor = AbstractController.masterExecutor;
        Object msgResp;
        try {
            msgResp = SmsSystem.askSync(actor, msg).get();
            PhysicalFlowResp res = (PhysicalFlowResp) msgResp;
            DagGraph vo = HDMViewAdapter.HDMPojoSeqToGraphAggregated(res.results());
            String json = ObjectUtils.objectToJson(vo);
            resp.setContentType("application/json");
            resp.getWriter().write(json);
        } catch (Exception e) {
            log(e.getCause().toString());
            resp.setContentType("application/json");
            resp.getWriter().write("");
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }
}
