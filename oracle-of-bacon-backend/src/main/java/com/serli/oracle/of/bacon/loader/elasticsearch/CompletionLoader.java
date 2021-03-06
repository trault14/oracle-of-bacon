package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
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

        String suggest = "[";
        if(firstname.length()>=1){
            suggest+="\""+firstname+"\"";
        }
        if(lastname.length()>=1){
            if(firstname.length()>=1){
                suggest+=",";
            }
            suggest+="\""+lastname+"\"";
        }
        if(nickname.length()>=1){
            if(lastname.length()>=1 || firstname.length()>=1){
                suggest+=",";
            }
            suggest+="\""+nickname+"\"";
        }
        suggest+="]";

        return "{\"nickname\":\""+nickname+"\", \"firstname\":\""+firstname+"\", \"lastname\":\""+lastname+"\", \"rawname\":\""+line+"\", \"suggest\":"+suggest+"}";
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();

        client.execute(new CreateIndex.Builder("actors").build());

        PutMapping putMapping = new PutMapping.Builder(
                "actors",
                "actor",
                "{ " +
                        "\"actor\" : { " +
                          "\"properties\" : { " +
                            "\"suggest\" : { " +
                              "\"type\" : \"completion\"" +
                            "}," +
                            "\"nickname\" : {" +
                              "\"type\" : \"string\"" +
                            "}," +
                            "\"firstname\" : {" +
                            "\"type\" : \"string\"" +
                            "}," +
                            "\"lastname\" : {" +
                            "\"type\" : \"string\"" +
                            "}" +
                          "}" +
                        "}" +
                        "}"
        ).build();
        JestResult result = client.execute(putMapping);
        System.out.println(result.getErrorMessage());

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
            bulkExecute(client, actors);
        } catch (IOException e){
            System.err.println("Error while reading csv file");
        }

        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
