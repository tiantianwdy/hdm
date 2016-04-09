package org.nicta.wdy.hdm.console.controllers;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.baidu.bpit.akka.server.SmsSystem;
import org.nicta.wdy.hdm.console.views.HDMViewAdapter;
import org.nicta.wdy.hdm.console.models.TreeVO;
import org.nicta.wdy.hdm.message.ApplicationsQuery;
import org.nicta.wdy.hdm.message.ApplicationsResp;

/**
 * Created by tiantian on 8/04/16.
 */
public class ApplicationController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ApplicationsQuery msg = new ApplicationsQuery();
        String actor = AbstractController.master;
        ApplicationsResp res = (ApplicationsResp) SmsSystem.askSync(actor, msg).get();
        TreeVO vo = HDMViewAdapter.applicationsRespToTreeVO(res);
        String json = ObjectUtils.objectToJson(vo);
        resp.setContentType("application/json");
        resp.getWriter().write(json);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }
}
