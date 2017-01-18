package com.serli.oracle.of.bacon.repository;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchRepository {

    private final JestClient jestClient;

    public ElasticSearchRepository() {
        jestClient = createClient();

    }

    public static JestClient createClient() {
        JestClient jestClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());

        jestClient = factory.getObject();
        return jestClient;
    }

    public List<String> getActorsSuggests(String searchQuery) {

        List<String> actorsSuggests = new ArrayList<>();

        String query = "{\n" +
                "    \"actor-suggest\" : {\n" +
                "        \"text\" : \""+searchQuery+"\",\n" +
                "        \"completion\" : {\n" +
                "            \"field\" : \"suggest\"\n" +
                "        }\n" +
                "    }\n" +
                "}";

        Suggest suggest = new Suggest.Builder(query)
                // multiple index or types can be added.
                .addIndex("actors")
                .build();

        try {
            SuggestResult result = jestClient.execute(suggest);
            List<SuggestResult.Suggestion> suggestions = result.getSuggestions("actor-suggest");

            List<Map<String, Object>> options = suggestions.get(0).options;

            for(Map<String, Object> option : options){
                Map<String, Object> value = (Map<String, Object>) option.get("_source");
                actorsSuggests.add(value.get("rawname").toString());
            }
        } catch (IOException e) {
            System.err.println("Error while calling Elasticsearch");
            return new ArrayList<>();
        }

        return actorsSuggests;
    }


}
