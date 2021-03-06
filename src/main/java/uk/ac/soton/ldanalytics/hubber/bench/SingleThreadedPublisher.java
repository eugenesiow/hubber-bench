package uk.ac.soton.ldanalytics.hubber.bench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class SingleThreadedPublisher {
	
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
        
        int max = 5400;
    	int maxmsgs = 5000;
        
		Queue<String> queue = new LinkedBlockingQueue<String>();
		Thread producer = new Thread(new Producer(queue,max,maxmsgs));
		Thread consumer = new Thread(new SingleConsumer(queue,subscribers,maxmsgs));
		producer.start();
		consumer.start();
    }
}

class SingleConsumer implements Runnable {
	Queue<String> queue;
	int count = 0;
	int sum = 0;
	int maxmsgs;
	Map<Integer,String[]> subscribers;
	SingleConsumer(Queue<String> queue, Map<Integer,String[]> subscribers ,  int maxmsgs){
		this.subscribers = subscribers;
		this.maxmsgs = maxmsgs;
		this.queue = queue;
	}

	public void run() {
		int qos             = 1;
	    String broker       = "tcp://localhost:1883";
	    MemoryPersistence persistence = new MemoryPersistence();
	    MqttClient sampleClient;
		String clientId     = UUID.randomUUID().toString();
		String topic        = "test/test";
		try {
			sampleClient = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
//	        connOpts.setCleanSession(true);
	        sampleClient.connect(connOpts);
	        String str;
	        LinkedBlockingQueue<String> q = (LinkedBlockingQueue<String>)queue;
			while ((str = q.take()) != null) {
				String[] spl = str.split(";");
				String[] allPubs = subscribers.get(Integer.parseInt(spl[0]));
				if(allPubs!=null) {
					for(String pub:allPubs) {
						MqttMessage message = new MqttMessage(spl[1].getBytes());
						message.setQos(qos);
						sampleClient.publish(topic+pub, message);
					}
				}
				String[] parts = spl[1].split(",");
				sum += System.currentTimeMillis() - Long.parseLong(parts[0]);
				count++;
				if(count>maxmsgs) {
					double avg = sum*1.0/count;
					sum = 0;
					count = 0;
					System.out.println(avg);
				}
			}
			System.out.println("finish");
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
}
