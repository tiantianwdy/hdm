package org.nicta.wdy.hdm.console.controllers;

import com.baidu.bpit.akka.configuration.ActorConfig;
import com.baidu.bpit.akka.messages.Query;
import com.baidu.bpit.akka.messages.Reply;
import com.baidu.bpit.akka.server.SmsSystem;
import org.nicta.wdy.hdm.console.models.TreeVO;
import org.nicta.wdy.hdm.console.views.HDMViewAdapter;
import org.nicta.wdy.hdm.message.AllSlavesQuery;
import org.nicta.wdy.hdm.message.ApplicationsResp;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

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
