package org.nicta.wdy.hdm.console.controllers;

/**
 * Created by tiantian on 8/04/16.
 */
public class AbstractController {

//    public static String master = "akka.tcp://masterSys@10.10.0.100:8999/user/smsMaster/";
    public static String master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/";
    public static String masterExecutor = master + "ClusterExecutor";
}
