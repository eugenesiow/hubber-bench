package uk.ac.soton.ldanalytics.hubber.bench;

public class Point {
	private String msg;
	private int publisher;
    
    public void set(int publisher, String msg) {
    	this.publisher = publisher;
        this.msg = msg;
    }
    
    public int getPublisher() {
    	return publisher;
    }
    
    public String getMsg() {
    	return msg;
    }
}

