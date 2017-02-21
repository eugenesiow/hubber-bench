package uk.ac.soton.ldanalytics.hubber.bench;

public class Point {
	private String msg;
	private Long publisher;
    
    public void set(Long publisher, String msg) {
    	this.publisher = publisher;
        this.msg = msg;
    }
    
    public long getPublisher() {
    	return publisher;
    }
    
    public String getMsg() {
    	return msg;
    }
}

