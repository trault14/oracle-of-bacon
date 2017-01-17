package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {

    public Jedis jedis = new Jedis("localhost");

    public void addSearch(String actorName){
        jedis.lpush("actors", actorName);
    }

    public List<String> getLastTenSearches() {
        return jedis.lrange("actors", 0, 9);
    }
}
