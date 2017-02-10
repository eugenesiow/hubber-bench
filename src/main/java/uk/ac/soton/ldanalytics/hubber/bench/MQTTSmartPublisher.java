package uk.ac.soton.ldanalytics.hubber.bench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MQTTSmartPublisher {
	public static void main(String[] args) {
		PublisherApplication("graphs/ca-GrQc-ps.txt");
    }
	
	private static void PublisherApplication(String input) {
		String broker       = "tcp://localhost:1883";
        String clientId     = "JavaSample";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
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
        	
            final MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected");
            
            final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(200);
            executorService.scheduleAtFixedRate(new Runnable() {
                
                public void run() {
                	System.out.println(System.currentTimeMillis());
//                	int count = 0;
                	for(Entry<Integer,String[]> subscriber:subscribers.entrySet()) {
                		for(String topushto:subscriber.getValue()) {
                			String content      = new Date().getTime() + ",An event just happened";
                    		MqttMessage message = new MqttMessage(content.getBytes());  
                    		message.setQos(0);
                    		try {
                    			sampleClient.publish("test/test"+Integer.parseInt(topushto), message);
    						} catch (MqttPersistenceException e) {
    							e.printStackTrace();
    						} catch (MqttException e) {
    							e.printStackTrace();
    						}
                		}
//                		System.out.println(count++);
                	}
                }
            }, 1, 1000, TimeUnit.MILLISECONDS);
            
//            sampleClient.disconnect();
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        } catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
