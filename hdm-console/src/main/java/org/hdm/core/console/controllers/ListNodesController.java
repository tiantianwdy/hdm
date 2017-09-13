package org.hdm.core.console.controllers;

import org.hdm.akka.configuration.ActorConfig;
import org.hdm.akka.messages.Query;
import org.hdm.akka.messages.Reply;
import org.hdm.akka.server.SmsSystem;
import org.hdm.core.console.models.TreeVO;
import org.hdm.core.console.views.HDMViewAdapter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 11/04/16.
 */
public class ListNodesController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Query msg = new Query("smsSystem/allSlaves", "", "", -1L, "", "");
        String actor = AbstractController.master;
        Reply res = (Reply) SmsSystem.askSync(actor, msg).get();
        scala.collection.Seq<ActorConfig> data = (scala.collection.Seq<ActorConfig>) res.result();
        TreeVO vo = HDMViewAdapter.slaveListToTreeVO(actor, data);
        String json = ObjectUtils.objectToJson(vo);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }
}
