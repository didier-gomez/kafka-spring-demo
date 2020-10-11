package com.dgomez.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;

import io.micrometer.core.instrument.MeterRegistry;

@SpringBootApplication
public class CursoKafkaSpringApplication {
	public static final String DEFAULT_TOPIC = "ecommerce-topic"; 
	public static Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	private KafkaListenerEndpointRegistry listenerRegistry;
	
	@Autowired
	private MeterRegistry meterRegistry;


	@KafkaListener(id="ecommerce-listener", topics = DEFAULT_TOPIC, 
			containerFactory = "concurrentListener", 
			properties = {"max.poll.interval.ms:4000", "max.poll.records:50"},
			groupId = "demo-group")
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Process batch started...");
		messages.stream().forEach(message -> {
			log.info("Received offset={}, key={}, msg={}",
					message.offset(), message.key(), message.value());
		});
		log.info("Processed {} messages", messages.size());
	}

	public static void main(String[] args) {
		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}
	
	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void scheduledMessages() {
		sendMessagesSimple("Purcharse operation", 200, 100);
	}

	

	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	private void printMetrics() {
		double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		log.info("count: {}", count);
	}
	
	private void sendMessagesSimple(String msg, int messageQuantity, int keyOffset) {
		for (int i = 0; i < messageQuantity; i++) {
			kafkaTemplate.send(DEFAULT_TOPIC, String.valueOf(i/keyOffset), String.format(msg + i));
		}
	}
	private void manualStartupDemo() throws InterruptedException {
		// Send messages first
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send(DEFAULT_TOPIC, String.valueOf(i/100), String.format("Purcharse operation %d", i));
		}
		// Delay 1 second
		Thread.sleep(1000);
		// Manually start listener
		log.info("Start listener");
		listenerRegistry.getListenerContainer("ecommerce-listener").start();
	}

	private void sendInitialMsgWithCallback() {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(DEFAULT_TOPIC, "initial message");
		future.addCallback(new KafkaSendCallback<String, String>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("Message sent {}", result.getRecordMetadata().offset());
			}
			@Override
			public void onFailure(Throwable ex) {
				log.error("Error sending message", ex.getMessage());
			}
			@Override
			public void onFailure(KafkaProducerException ex) {
				log.error("Error sending message", ex.getMessage());
			}
		});
	}

}
