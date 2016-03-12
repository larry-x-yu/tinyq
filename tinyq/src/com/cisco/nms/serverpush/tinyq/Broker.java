/**
 * 
 */
package com.cisco.nms.serverpush.tinyq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author laryu
 *
 */

@ServerEndpoint(value = "/tinyq")
public class Broker {

	private static final String ACTION_PUB = "pub";
	private static final String ACTION_SUB = "sub";
	private static final String ACTION_UNSUB = "unsub";
	private static final String MESSAGE = "message";
	private static final String TOPIC = "topic";
	private static final int TIMEOUT_SEND = 5; // milliseconds 
		
    private static final Log log = LogFactory.getLog(Broker.class);

    private static final Map<String, Broker> connections = new ConcurrentHashMap<String, Broker>();

    private static Map<String, Set<String>> subscriptions = new ConcurrentHashMap<String, Set<String>>();
    
    private Session session;

    @OnOpen
    public void start(Session session, EndpointConfig epc) {
    	if(epc != null) {
    		log.debug("EndpointConfig = '" + epc + "'");
    	}
        this.session = session;
        if(!connections.keySet().contains(this.session.getId())) {
        	connections.put(this.session.getId(), this);
        	log.debug(session.getId() + " connected");
        }
    }


    @OnClose
    public void end() {
    	try {
    		if(this.session.isOpen()) {
    			this.session.close();
    		}
    		connections.remove(this);
    		log.debug(session.getId() + " disconnected");
    	}
    	catch(Exception e) {
    		//Ingore exception
    	}
    }

    @OnMessage
    public void onMessage(String message) {
    	Map<String, String> nameValuePairs = this.getNameValuePairs(message);
		String topic = nameValuePairs.get(TOPIC);
		Set<String> subs = subscriptions.get(topic);
		if(topic != null && !"".equals(topic.trim())) {
	    	if(nameValuePairs.values().contains(ACTION_SUB)) {//
	    		if(subs == null) {
	    			subs = new HashSet<String>();
	    		}
    			subs.add(this.session.getId());
    			subscriptions.put(topic, subs);
	    	}
	    	else if(nameValuePairs.values().contains(ACTION_UNSUB)) {
	    		if(subs != null) {
	    			subs.remove(this.session.getId());
	    			subscriptions.put(topic, subs);	  
	    		}
	    		else {
	    			log.error("Error: trying to unsubscribe from a topic that no one has subscribed to.");
	    		}
	    	}
	    	else if(nameValuePairs.values().contains(ACTION_PUB)) {
	    		if(subs != null) {
	       			List<String> toRemove = new ArrayList<String>();
	       		    for(String id : subs) {
		    			Broker b = connections.get(id);
		    			if(b != null) {
		    				//b.sendMessage(nameValuePairs.get(MESSAGE));
		    				b.sendMessage(message);
		    			}
		    			else {
		    				toRemove.add(id);
		    			}
		    		}
		    		
		    		if(!toRemove.isEmpty()) {
		    			subs.removeAll(toRemove);
		    			subscriptions.put(topic, subs);	  
		    		}		    		
	    		}
	    	}			
		}
		else {
			log.error("Invalid message received: " + message);
		}
    }


    @OnError
    public void onError(Throwable t) throws Throwable {
        log.error("ServerPush Broker Error: " + t.toString(), t);
    }


    private Map<String, String> getNameValuePairs(String msg) {
   		Map<String, String> nameValuePairs = new HashMap<String, String>();
       	if(msg !=null) {
    		StringTokenizer st = new StringTokenizer(msg, "&");
    		
    		while(st.hasMoreTokens()) {
    			String token = st.nextToken();
    			if(token.contains("=")) {
    				StringTokenizer st2 = new StringTokenizer(token, "=");
    				nameValuePairs.put(st2.nextToken().toLowerCase(), st2.nextToken());
    			}
    			else {
    				log.error("Invalid segment: " + token);
    			}
    		}
       	}
       	
       	return nameValuePairs;
    }
    
    protected String formMessage(Map<String, String> nvpairs) {
    	String msg = null;
    	
    	if(nvpairs != null && !nvpairs.isEmpty()) {
    		if(nvpairs.get(TOPIC) != null && nvpairs.get(MESSAGE) != null) {
    			msg = TOPIC + "=" + nvpairs.get(TOPIC) + "&" + MESSAGE + "=" + nvpairs.get(MESSAGE);
    		}
    		else {
    			throw new RuntimeException("Invalid message received: " + nvpairs.toString());
    		}
    	}
    	
    	return msg;
    }

    public void sendMessage(String message) {
    	synchronized (session) {
    		try {
    			session.getBasicRemote().sendText(message);
    		} catch (Exception e) {
    			log.info(this.session.getId() + " removed from queue due to IO exception: assuming dead peer.");
    			connections.remove(this.session.getId());
    		}
    	}
    }
    
//	public void sendMessage(String message) {
//		Future<Void> f = null;
//		synchronized (session) {
//			f = session.getAsyncRemote().sendText(message);
//		}
//		boolean done = false;
//		while(!done) {
//			try {
//				f.get(TIMEOUT_SEND, TimeUnit.MILLISECONDS);
//			}
//			catch (ExecutionException e) {
//				done = true;
//				connections.remove(this.session.getId());
//			} catch (Exception e) {
//				done = true;
//				if(f.isDone()) {
//				}
//				else if(f.isCancelled()) {
//					connections.remove(this.session.getId());
//					log.debug("Removed connection '" + session.getId() + "'");
//				}
//				else {
//					done = false;
//				}
//			}			
//		}
//	}
	
}
