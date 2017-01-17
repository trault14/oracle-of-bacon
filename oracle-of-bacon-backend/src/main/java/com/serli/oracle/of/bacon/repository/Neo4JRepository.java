package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.*;

import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;

import java.util.ArrayList;
import java.util.List;


public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
    }

    public List<?> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();

        Transaction tx = session.beginTransaction();
        String query = "MATCH p=shortestPath(\n" +
                "  (bacon:Actor {name:\"Bacon, Kevin (I)\"})-[*]-(meg:Actor {name:\"Abadie\"})\n" +
                ")\n" +
                "RETURN p";
        StatementResult result = tx.run(query);

        System.out.println("SYSOU REPERE");
        List<GraphItem> graphItemList = new ArrayList<>();
        if (result.hasNext()) {
            Path path = result.next().get("p").asPath();
            System.out.println(path);
            path.nodes().forEach( node -> {
                graphItemList.add(new GraphNode(node.id(), node.values().toString(), node.labels().toString()));
                //System.out.println(node.id());
                System.out.println(node.values().toString());

            });
            /*path.relationships().forEach(relationship -> {
                graphItemList.add(new GraphEdge());
            });*/
        }
        System.out.println(graphItemList);

        driver.close();
        return graphItemList;

        //System.out.println( String.format( "%s %s", record.get("p").get( "type" ).asString(), record.get("value").asString() ) );
    }

    private static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
