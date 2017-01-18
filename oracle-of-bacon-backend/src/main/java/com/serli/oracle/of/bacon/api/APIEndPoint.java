package com.serli.oracle.of.bacon.api;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import net.codestory.http.annotations.Get;
import org.bson.Document;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {
        redisRepository.addSearch(actorName); //Add search to Redis

        List<?> graphItemList = neo4JRepository.getConnectionsToKevinBacon(actorName);

        String str = "[";
        Iterator iterator = graphItemList.iterator();
        while (iterator.hasNext()){
            Object graphItem = iterator.next();
            str += graphItem.toString();
            if(iterator.hasNext()){
                str += ",";
            }
        }

        str+= "]";
        return str;
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) {
        return elasticSearchRepository.getActorsSuggests(searchQuery);
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return redisRepository.getLastTenSearches();
    }

    /**
     *
     * @param actorName in the form "Name Surname"
     * @return actor description
     */
    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
        Optional<Document> myDoc = mongoDbRepository.getActorByName(actorName);
        return myDoc.get().toJson();
    }
}
