package com.dgomez.kafka;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public static Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@KafkaListener(topics = "ecommerce-topic", containerFactory = "concurrentListener", properties = {
			"max.poll.interval.ms:1000", "max.poll.records:100" }, groupId = "demo-group")
	public void listen(List<String> messages) {
		log.info("Process batch started...");
		messages.stream().forEach(message -> log.info("Message received: {}", message));
		log.info("Processed {} messages", messages.size());
	}

	public static void main(String[] args) {
		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// sendInitialMsgWithCallback();
		for (int i = 0; i < 500; i++) {
			kafkaTemplate.send("ecommerce-topic", String.format("Purcharse operation %d", i));
		}
	}

	private void sendInitialMsgWithCallback() {

		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("ecommerce-topic", "initial message");
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
