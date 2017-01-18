package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    private static BulkResult bulkExecute(JestClient client, List<Index> list){
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("actors")
                .defaultType("Actor")
                .addAction(list)
                .build();
        try {
            return client.execute(bulk);
        } catch (IOException e) {
            System.err.println("Bulk execution failed");
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();

        List<Index> actors = new ArrayList<Index>();

        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader.lines().skip(1)
                    .forEach(line -> {
                        String source = "{\"name\":"+line+"}";
                        System.out.println(source);
                        actors.add(new Index.Builder(source).build());
                        count.incrementAndGet();
                        if(actors.size()%50000==0){
                            bulkExecute(client, actors);
                            actors.clear();
                        }
                    });
        }

        bulkExecute(client, actors);

        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
