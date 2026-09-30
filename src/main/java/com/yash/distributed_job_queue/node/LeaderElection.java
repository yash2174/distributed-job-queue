package com.yash.distributed_job_queue.node;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class LeaderElection {

    private static final String LEADER_KEY = "queue:leader";

    private final StringRedisTemplate redisTemplate;

    private final String nodeId = UUID.randomUUID().toString();

    public LeaderElection(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean tryBecomeLeader() {

        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(
                        LEADER_KEY,
                        nodeId,
                        10,
                        TimeUnit.SECONDS
                );

        return Boolean.TRUE.equals(acquired);
    }
    public boolean isLeader() {
    String currentLeader = redisTemplate.opsForValue()
            .get(LEADER_KEY);

    return nodeId.equals(currentLeader);
}
public void renewLeadership() {

    if (isLeader()) {
        redisTemplate.expire(
                LEADER_KEY,
                10,
                TimeUnit.SECONDS
        );
    }
}
}