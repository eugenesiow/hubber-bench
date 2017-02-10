package uk.ac.soton.ldanalytics.hubber.bench;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MQTTPublisher {
	public static void main(String[] args) {
        final String broker       = "tcp://localhost:1883";       
            
            
            final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(10);
            executorService.scheduleAtFixedRate(new Runnable() {
            	
                
	            public void run() {
	            	try {
	            		String clientId     = "JavaSample" + UUID.randomUUID();
	            		MemoryPersistence persistence = new MemoryPersistence();
	                	final MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
	                    MqttConnectOptions connOpts = new MqttConnectOptions();
	                    connOpts.setCleanSession(true);
	                    System.out.println("Connecting to broker: "+broker);
	                    sampleClient.connect(connOpts);
	                    System.out.println("Connected");
	                	System.out.println(System.currentTimeMillis());
	                	for(int i=0;i<7500;i++) {
	                		String content      = new Date().getTime() + ",An event just happened";
	                		MqttMessage message = new MqttMessage(content.getBytes());  
	                		message.setQos(1);
	                        try {
								sampleClient.publish("test/test"+i, message);
							} catch (MqttPersistenceException e) {
								e.printStackTrace();
							} catch (MqttException e) {
								e.printStackTrace();
							}
	                	}
	                	sampleClient.disconnect();
	                } catch(MqttException me) {
	                    System.out.println("reason "+me.getReasonCode());
	                    System.out.println("msg "+me.getMessage());
	                    System.out.println("loc "+me.getLocalizedMessage());
	                    System.out.println("cause "+me.getCause());
	                    System.out.println("excep "+me);
	                    me.printStackTrace();
	                }
	            }
            }, 1, 1000, TimeUnit.MILLISECONDS);
            
//            sampleClient.disconnect();
        
    }
}
