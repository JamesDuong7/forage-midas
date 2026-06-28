package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("RECEIVED TRANSACTION: senderId={}, recipientId={}, amount={}", 
                transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount());

        UserRecord sender = databaseConduit.findById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            logger.warn("Transaction discarded: Invalid sender or recipient ID.");
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Transaction discarded: Sender has insufficient balance (Balance: {}, Requested: {}).", 
                    sender.getBalance(), transaction.getAmount());
            return;
        }

        // Deduct from sender, add to recipient
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save adjusted balances
        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        // Record transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        databaseConduit.save(record);

        logger.info("Transaction processed successfully.");

        // Log waldorf's balance
        UserRecord waldorf = databaseConduit.findById(5L);
        if (waldorf != null) {
            logger.info("WALDORF CURRENT BALANCE: {}", waldorf.getBalance());
        }
    }
}