package org.hdm.core.console.controllers;

import org.hdm.akka.messages.Query;
import org.hdm.akka.messages.Reply;
import org.hdm.akka.server.SmsSystem;
import org.hdm.core.console.views.HDMViewAdapter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tiantian on 11/04/16.
 */
public class NodeMonitorController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String prop = req.getParameter("prop");
        String[] key = req.getParameterValues("key");
        List results = new ArrayList();
        for(String k:key){
            Query msg = new Query("dataService/dataLike", prop, k, 1800*1000L, "", "");
            String actor = AbstractController.master;
            Reply reply = (Reply) SmsSystem.askSync(actor, msg).get();

            Object[][] data = HDMViewAdapter.slaveMonitorVO(reply.result());
            results.add(data);
        }
//        scala.collection.Seq<ActorConfig> data = (scala.collection.Seq<ActorConfig>) res.result();

        String json = ObjectUtils.objectToJson(results);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }
}
