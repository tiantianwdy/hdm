package org.hdm.core.console.controllers;

import org.hdm.akka.server.SmsSystem;
import org.hdm.core.console.models.TreeVO;
import org.hdm.core.console.views.HDMViewAdapter;
import org.hdm.core.messages.AllAppVersionsQuery;
import org.hdm.core.messages.AllAppVersionsResp;


import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by tiantian on 12/04/16.
 */
public class AllAppVersionsController extends HttpServlet{

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        AllAppVersionsQuery msg = new AllAppVersionsQuery();
        String actor = AbstractController.masterExecutor;
        AllAppVersionsResp res = (AllAppVersionsResp) SmsSystem.askSync(actor, msg).get();
        TreeVO vo = HDMViewAdapter.allVersionsRespToTreeVO(res);
        String json = ObjectUtils.objectToJson(vo);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

}
