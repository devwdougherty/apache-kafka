package com.github.devwdougherty.twitterproducerclient;

import com.github.devwdougherty.twitterproducerclient.producer.TwitterProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TwitterProducerClientApplication {

	public static void main(String[] args) {
		SpringApplication.run(TwitterProducerClientApplication.class, args);

		TwitterProducer twitterProducer = new TwitterProducer();

		twitterProducer.producerTwitter();
	}
}
