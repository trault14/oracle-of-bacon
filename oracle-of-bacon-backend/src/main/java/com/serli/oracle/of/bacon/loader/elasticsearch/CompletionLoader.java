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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static String createJson(String line){
        String firstname = "";
        String lastname = "";
        String nickname = "";
        String rawname = line;

        line = line.replace("\"", "");
        String[] splittedLine = line.split(", ");
        if(splittedLine.length == 1){
            nickname = splittedLine[0];
        }
        else {
            lastname = splittedLine[0];
            firstname = splittedLine[1];
            Pattern p = Pattern.compile("'*'");
            Matcher m = p.matcher(splittedLine[0]);
            if (m.find()) {
                nickname = m.group();
                lastname = splittedLine[0].replace(nickname, "");
            }
        }

        return "{\"nickname\":\""+nickname+"\", \"firstname\":\""+firstname+"\", \"lastname\":\""+lastname+"\", \"rawname\":\""+line+"\"}";
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();

        /*
        PutMapping putMapping = new PutMapping.Builder(
                "actors",
                "Actor",
                "{ \"Actor\" : { \"properties\" : { \"name\" : {\"type\" : \"string\"} } } }"
        ).build();
        client.execute(putMapping);
        */

        List<Index> actors = new ArrayList<Index>();

        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader.lines().skip(1)
                    .forEach(line -> {

                        String source = createJson(line);

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
