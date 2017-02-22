package uk.ac.soton.ldanalytics.hubber.bench;

import java.io.BufferedReader;
import java.io.FileReader;
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

public class ThreadedPublisher {
	static String topic        = "MQTT Examples";
    static int qos             = 1;
    static String broker       = "tcp://localhost:1883";
    static MemoryPersistence persistence = new MemoryPersistence();
    static MqttClient sampleClient;
	
	public static void main(String[] args) throws Exception {
		String input = "graphs/ca-GrQc-ps.txt";
		final Map<Integer,String[]> subscribers = new HashMap<Integer,String[]>();
        
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
		int max = 5400;
		int maxmsgs = 1000;
		String clientId     = UUID.randomUUID().toString();
		try {
			sampleClient = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
//	        connOpts.setCleanSession(true);
	        sampleClient.connect(connOpts);
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
        while (!Thread.currentThread ().isInterrupted()) {
        	for(int i=0;i<maxmsgs;i++) {
        		int pubs = rand.nextInt(max);
        		String msg = System.currentTimeMillis() + ","+",An event just happened";
    			MqttMessage message = new MqttMessage(msg.getBytes());
    			message.setQos(qos);
    			sampleClient.publish(topic, message);
        	}
        	Thread.sleep(1000);
        }
    }
}
