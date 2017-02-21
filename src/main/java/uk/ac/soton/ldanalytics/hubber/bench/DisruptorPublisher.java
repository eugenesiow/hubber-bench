package uk.ac.soton.ldanalytics.hubber.bench;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorPublisher {
	public static void handleEvent(Point event, long sequence, boolean endOfBatch) {
		System.out.println(event.getMsg());
    }

    public static void translate(Point event, long sequence, String msg) {
    	event.set(msg);
    }
	
	public static void main(String[] args) throws Exception {
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        Disruptor<Point> disruptor = new Disruptor<>(Point::new, bufferSize, executor); //TODO: change to threadfactory

        // Connect the handler
        disruptor.handleEventsWith(DisruptorPublisher::handleEvent);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<Point> ringBuffer = disruptor.getRingBuffer();
        

        while (!Thread.currentThread ().isInterrupted ()) {
        	String msg = "test";
        	ringBuffer.publishEvent(DisruptorPublisher::translate, msg);
        }
    }
}
