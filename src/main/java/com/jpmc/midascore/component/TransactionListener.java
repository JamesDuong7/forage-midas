package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final List<Float> firstFourAmounts = new ArrayList<>();

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("RECEIVED TRANSACTION: senderId={}, recipientId={}, amount={}",
                transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());

        if (firstFourAmounts.size() < 4) {
            firstFourAmounts.add(transaction.getAmount());
            logger.info("AMOUNT #{}: {}", firstFourAmounts.size(), transaction.getAmount());
        }

        if (firstFourAmounts.size() == 4) {
            logger.info("FIRST FOUR AMOUNTS: {}", firstFourAmounts);
        }
    }
}