package uk.ac.soton.ldanalytics.hubber.bench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MultiThreadedPublisher {
	
	public static void main(String[] args) throws Exception {
		String input = "graphs/wiki-Vote-ps.txt";
		final Map<Integer,String[]> subscribers = new HashMap<Integer,String[]>();
		AtomicInteger count = new AtomicInteger(0);
		AtomicInteger sum = new AtomicInteger(0);

        BufferedReader br = new BufferedReader(new FileReader(input));
        String line = "";
		while((line = br.readLine())!=null) {
			String[] parts = line.split("\t");
			if(parts.length>1) {
				subscribers.put(Integer.parseInt(parts[0]), parts[1].trim().split(","));
			}
		}
        br.close();
        
        int max = 7100;
    	int maxmsgs = 10000;
        
		Queue<String> queue = new LinkedBlockingQueue<String>();
		Thread producer = new Thread(new Producer(queue,max,maxmsgs));
		producer.start();
		ExecutorService threadPool = Executors.newFixedThreadPool(10);
		for(int i=0;i<10;i++) {
			threadPool.execute(new Consumer(queue,subscribers,maxmsgs,count,sum));
		}
    }
}

class Producer implements Runnable {
	Queue<String> queue;
	Random rand = new Random();
	int maxmsgs;
	int max;
	Producer(Queue<String> queue, int max, int maxmsgs){
		this.queue = queue;
		this.maxmsgs = maxmsgs;
		this.max = max;
	}
	public void run() {
		try {
			for(int k=0;k<10;k++) {
	        	String msg = System.currentTimeMillis() +",An event just happened";
	        	for(int i=0;i<maxmsgs;i++) {
	        		int pubs = rand.nextInt(max);
	        		queue.offer(pubs+";"+msg);
	        	}
	        	Thread.sleep(1000);
	        }
		} catch(Exception e) {
			e.printStackTrace();
		}
		
   }
}

class Consumer implements Runnable {
	Queue<String> queue;
	int maxmsgs;
	AtomicInteger count;
	AtomicInteger sum;
	Map<Integer,String[]> subscribers;
	Consumer(Queue<String> queue, Map<Integer,String[]> subscribers ,  int maxmsgs, AtomicInteger count, AtomicInteger sum){
		this.subscribers = subscribers;
		this.maxmsgs = maxmsgs;
		this.queue = queue;
		this.count = count;
		this.sum = sum;
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
				sum.addAndGet((int)(System.currentTimeMillis() - Long.parseLong(parts[0])));
				count.incrementAndGet();
				if(count.get()>maxmsgs) {
					double avg = sum.get()*1.0/count.get();
					sum.set(0);
					count.set(0);
					System.out.println(avg);
				}
			}
			System.out.println("finish");
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
}
