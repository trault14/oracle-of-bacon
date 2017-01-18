package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.Optional;
import java.util.regex.Pattern;

public class MongoDbRepository {

    private final MongoClient mongoClient;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
    }

    public Optional<Document> getActorByName(String name) {
        MongoDatabase database = mongoClient.getDatabase("actors");
        MongoCollection<Document> collection = database.getCollection("things");

        Pattern searchRegex = Pattern.compile(".*" + name + ".*");
        Document myDoc = collection.find(Filters.regex("name:ID", searchRegex)).first();
        return Optional.ofNullable(myDoc);
    }
}
