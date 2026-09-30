package com.yash.distributed_job_queue.node;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class NodeManager {

    private final StringRedisTemplate redisTemplate;

    private final String nodeId = UUID.randomUUID().toString();

    @Value("${server.port:8080}")
    private int port;

    public NodeManager(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    @Scheduled(fixedRate = 5000)
    public void registerNode() {

        String key = "node:" + nodeId;

        redisTemplate.opsForValue().set(
                key,
                "localhost:" + port,
                10,
                TimeUnit.SECONDS
        );

        System.out.println(
                "Node registered: " + nodeId + " on port " + port
        );
    }
}
