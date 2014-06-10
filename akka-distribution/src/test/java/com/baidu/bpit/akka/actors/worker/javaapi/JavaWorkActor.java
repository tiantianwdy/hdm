package com.baidu.bpit.akka.actors.worker.javaapi;


public class JavaWorkActor extends JWorkActor {

    public JavaWorkActor(Object params) {
        super(params);
    }

    public int initiated(Object params) {
		// TODO Auto-generated method stub
         return 0;
	}

	public void messageReceived(Object msg) {
		// TODO Auto-generated method stub

	}

    @Override
    public void updateParams(String confId, Object params, boolean discardOld) {

    }

    public static void main(String[] args) {
		
		System.out.println("test");
	}

}
