package uk.ac.soton.ldanalytics.hubber.bench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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
	static String topic        = "test/test";
    static int qos             = 1;
    static String broker       = "tcp://localhost:1883";
    static MemoryPersistence persistence = new MemoryPersistence();
    static MqttClient sampleClient;
    static Map<Integer,String[]> subscribers;
    static int count = 0;
	static int sum = 0;
	static int max = 5400;
	static int maxmsgs = 1000;
    
	
	public static void handleEvent(Point event, long sequence, boolean endOfBatch) {
		try {
			MqttMessage message = new MqttMessage(event.getMsg().getBytes());
			message.setQos(qos);
			String[] pubs = subscribers.get(event.getPublisher());
			if(pubs!=null) {
//				System.out.println(subscribers.get(1)[1]);
				for(String pub:pubs) {
					sampleClient.publish(topic, message);
				}
				String[] parts = event.getMsg().split(",");
				sum += System.currentTimeMillis() - Long.parseLong(parts[0]);
    			count++;
			}
			if(count>maxmsgs) {
				double avg = sum*1.0/count;
				sum = 0;
				count = 0;
				System.out.println(avg);
			}
		} catch (MqttException e) {
			e.printStackTrace();
		}
    }

    public static void translate(Point event, long sequence, String[] msg) {
    	event.set(Integer.parseInt(msg[0]),msg[1]);
    }
	
	public static void main(String[] args) throws Exception {
		String input = "graphs/ca-GrQc-ps.txt";
        
		subscribers = new HashMap<Integer,String[]>();
        BufferedReader br = new BufferedReader(new FileReader(input));
        String line = "";
		while((line = br.readLine())!=null) {
			String[] parts = line.split("\t");
			if(parts.length>1) {
				subscribers.put(Integer.parseInt(parts[0]), parts[1].trim().split(","));
			}
		}
        br.close();
		
		Random rand = new Random();
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
        int bufferSize = 64;

        // Construct the Disruptor
        Disruptor<Point> disruptor = new Disruptor<>(Point::new, bufferSize, executor); //TODO: change to threadfactory

        // Connect the handler
        disruptor.handleEventsWith(DisruptorPublisher::handleEvent);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<Point> ringBuffer = disruptor.getRingBuffer();
        

        for(int k=0;k<10;k++) {
        	for(int i=0;i<maxmsgs;i++) {
//        		String[] msg = {Integer.toString(rand.nextInt(max)),new Date().getTime()+",An event just happened"+i};
        		String[] msg = {Integer.toString(rand.nextInt(max)),System.currentTimeMillis()+",An event just happened"};
//        		String[] msg = {"0",System.currentTimeMillis()+",An event just happened"};
        		ringBuffer.publishEvent(DisruptorPublisher::translate, msg);
        	}
        	Thread.sleep(1000);
        }
        disruptor.shutdown();
    }
}
