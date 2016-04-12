package org.nicta.wdy.hdm.console.controllers;

import com.baidu.bpit.akka.server.SmsSystem;
import org.nicta.wdy.hdm.console.models.TreeVO;
import org.nicta.wdy.hdm.console.views.HDMViewAdapter;
import org.nicta.wdy.hdm.message.AllAppVersionsQuery;
import org.nicta.wdy.hdm.message.AllAppVersionsResp;
import org.nicta.wdy.hdm.message.ApplicationsQuery;
import org.nicta.wdy.hdm.message.ApplicationsResp;

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
