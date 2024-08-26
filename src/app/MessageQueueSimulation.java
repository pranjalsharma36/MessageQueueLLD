package app;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageQueueSimulation {
	
	public static void main(String[] args) {
		
		Producer p1 = new Producer("Pranjal");
		Consumer c1 = new Consumer("Sharma");
		
		List<Topic> topics = new ArrayList<>();
		
		List<Consumer> consumers = new ArrayList<>();
		List<Producer> producers = new ArrayList<>();
		
		consumers.add(c1);
		producers.add(p1);
		
		MessageQueueService service = new MessageQueueService();
		service.createTopic(producers, consumers, "topic1");
		
		Topic t1 = service.getTopic("topic1");
		
		topics.add(t1);

		ExecutorService executorService = Executors.newFixedThreadPool(10);
		executorService.submit(new ProducerTask(p1, t1, service));
		
		executorService.submit(new ConsumerTask(c1, service));
	}
}

class Message{
	String message;
	
	public Message(String message) {
		// TODO Auto-generated constructor stub
		this.message = message;
	}
}

class MessageQueueService{
	
	TopicManager topicManager;
	
	public MessageQueueService() {
		// TODO Auto-generated constructor stub
		topicManager = new TopicManager();
	}
	
	public void createTopic(List<Producer> producers, List<Consumer> consumers, String id) {
		topicManager.createTopic(producers, consumers, id);
	}
	
	public void addMessageToTopic(String id, Message message, Producer producer) {
		topicManager.addMessageToTopic(id, message, producer);
	}
	
	public Message getMessageFromTopic(String id) {
		return topicManager.getMessageFromTopic(id);
	}
	
	public Topic getTopic(String id) {
		return topicManager.get(id);
	}
	
}

class TopicManager{
	
	HashMap<String, Topic> topicCatalog;
	
	public TopicManager() {
		// TODO Auto-generated constructor stub
		this.topicCatalog = new HashMap<>();
	}
	
	public void createTopic(List<Producer> producers, List<Consumer> consumers, String id) {
		Topic topic = new Topic(producers, consumers, id);
		topicCatalog.put(id, topic);
	}
	
	public Topic getTopic(String id) {
		return topicCatalog.get(id);
	}
	
	public void addMessageToTopic(String id, Message message, Producer producer) {
		try {
			// TODO : check if the producer belongs to the topic or not
			topicCatalog.get(id).addMessage(message);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Message getMessageFromTopic(String id) {
		Topic topic = topicCatalog.get(id);
		try {
			return topic.getMessage();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Topic get(String id) {
		return topicCatalog.get(id);
	}
}

class Topic {
	BlockingQueue<Message> queue;
	
	List<Producer> producers;
	List<Consumer> consumers;
	
	String id;
	
	public Topic(List<Producer> producers, List<Consumer> consumers, String id) {
		// TODO Auto-generated constructor stub
		queue = new ArrayBlockingQueue<Message>(100);
		this.producers = producers;
		this.consumers = consumers;
		
		for(Consumer consumer : consumers) {
			consumer.addTopic(this);
		}
		this.id = id;
	}
	
	public Message getMessage() throws InterruptedException {
		
		Message message = queue.take();
		
		return message;
	}
	
	public void addMessage(Message message) throws InterruptedException {
		
		queue.put(message);
		System.out.println("Message published to topic " + message.message);
	}
	
}

class ProducerTask implements Runnable{
	
	Producer producer;
	Topic topic;
	MessageQueueService service;
	
	
	
	public ProducerTask(Producer producer, Topic topic, MessageQueueService service) {
		super();
		this.producer = producer;
		this.topic = topic;
		this.service = service;
	}




	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		for(int i = 0; i < 5; i++) {
			
			Message message = new Message("Message Published by " + producer.name + " count " + i);
			try {
//				topic.addMessage(message);
				service.addMessageToTopic(topic.id, message, producer);
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}

class Producer{
	
	String name;

	public Producer(String name) {
		super();
		this.name = name;
	}
	
	
}
class Consumer {
	List<Topic> topics;
	String name;

	public Consumer(String name) {
		super();
		this.topics = new ArrayList<Topic>();
		this.name = name;
	}
	
	public void addTopic(Topic topic) {
		topics.add(topic);
	}
}

// TODO : we can also introduce a consumer group in between

class ConsumerTask implements Runnable{
	
	Consumer consumer;
	MessageQueueService service;

	public ConsumerTask(Consumer consumer, MessageQueueService service) {
		super();
		this.consumer = consumer;
		this.service = service;
	}




	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		List<Topic> topics = consumer.topics;
		
		for(Topic topic : topics) {
			
			while(true) {
				try {
//					Message message = topic.getMessage();
					Message message = service.getMessageFromTopic(topic.id);
					
					System.out.println(message.message + "  " + consumer.name);
					
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
}