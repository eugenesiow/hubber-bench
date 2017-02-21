package uk.ac.soton.ldanalytics.hubber.bench;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorPublisher {
	static String topic        = "MQTT Examples";
    static int qos             = 1;
    static String broker       = "tcp://iot.eclipse.org:1883";
    static MemoryPersistence persistence = new MemoryPersistence();
    static MqttClient sampleClient;
    
	
	public static void handleEvent(Point event, long sequence, boolean endOfBatch) {
		try {
			System.out.println(event.getMsg());
			MqttMessage message = new MqttMessage(event.getMsg().getBytes());
			message.setQos(qos);
			sampleClient.publish(topic, message);
		} catch (MqttException e) {
			e.printStackTrace();
		}
    }

    public static void translate(Point event, long sequence, String[] msg) {
    	event.set(Long.parseLong(msg[0]),msg[1]);
    }
	
	public static void main(String[] args) throws Exception {
		String clientId     = UUID.randomUUID().toString();
		try {
			sampleClient = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
//	        connOpts.setCleanSession(true);
	        sampleClient.connect(connOpts);
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
		
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
        	String[] msg = {"0","test"};
        	ringBuffer.publishEvent(DisruptorPublisher::translate, msg);
        }
    }
}
