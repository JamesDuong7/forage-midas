package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    private final RestTemplate restTemplate;

    public IncentiveService(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public float getIncentive(Transaction transaction) {
        String url = "http://localhost:8080/incentive";
        try {
            Incentive response = restTemplate.postForObject(url, transaction, Incentive.class);
            if (response != null) {
                return response.getAmount();
            }
        } catch (Exception e) {
            // Log warning or handle exception
        }
        return 0f;
    }
}
