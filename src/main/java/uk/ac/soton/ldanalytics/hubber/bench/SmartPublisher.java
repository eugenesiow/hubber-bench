package uk.ac.soton.ldanalytics.hubber.bench;

import io.deepstream.ConnectionState;
import io.deepstream.ConnectionStateListener;
import io.deepstream.DeepstreamClient;
import io.deepstream.DeepstreamRuntimeErrorHandler;
import io.deepstream.Event;
import io.deepstream.InvalidDeepstreamConfig;
import io.deepstream.LoginResult;
import io.deepstream.Topic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonParser;

public class SmartPublisher {
    public static void main(String[] args) throws InvalidDeepstreamConfig, InterruptedException {
        new PublisherApplication("graphs/ca-GrQc-ps.txt");
        //new PublisherApplication("graphs/wiki-Vote-ps.txt");
    }

    static class PublisherApplication {

        PublisherApplication(String input) throws InvalidDeepstreamConfig {

            try {
//                final DeepstreamClient client = new DeepstreamClient("localhost:6020");
            	final DeepstreamClient client = new DeepstreamClient("192.168.0.100:6020");
                subscribeConnectionChanges(client);
                subscribeRuntimeErrors(client);
                
                JsonParser g = new JsonParser();
                LoginResult loginResult = client.login(g.parse("{\"username\":\"\",\"password\":\"\"}"));
                
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
                
                if (!loginResult.loggedIn()) {
                    System.err.println("Provider Failed to login " + loginResult.getErrorEvent());
                } else {
                    System.out.println("Provider Login Success");
                    final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
                    executorService.scheduleAtFixedRate(new Runnable() {
                        
                        public void run() {
                        	for(Entry<Integer,String[]> subscriber:subscribers.entrySet()) {
                        		for(String topushto:subscriber.getValue())
                        			client.event.emit("test/test"+Integer.parseInt(topushto), new Object[]{"An event just happened", new Date().getTime()});
                        	}
                        }
                    }, 1, 1000, TimeUnit.MILLISECONDS);
                    
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        private void subscribeRuntimeErrors(DeepstreamClient client) {
            client.setRuntimeErrorHandler(new DeepstreamRuntimeErrorHandler() {
                public void onException(Topic topic, Event event, String errorMessage) {
                    System.out.println(String.format("Error occured %s %s %s", topic, event, errorMessage));
                }
            });
        }

        private void subscribeConnectionChanges(DeepstreamClient client) {
            client.addConnectionChangeListener(new ConnectionStateListener() {
                public void connectionStateChanged(ConnectionState connectionState) {
                    System.out.println("Connection state changed " + connectionState);
                }
            });
        }

    }
}
