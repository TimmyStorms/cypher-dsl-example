package com.github.timmystorms.cypherdsl;

import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentGraphDatabase;

/**
 * Executes cypher statements without Cypher-DSL.
 * 
 * @author Timmy Storms (timmy dot storms at gmail dot com)
 */
public final class CypherTest {

    private GraphDatabaseService db;

    private ExecutionEngine executionEngine;

    /**
     * Initialises {@link ImpermanentGraphDatabase} and {@link ExecutionEngine}.
     */
    @Before
    public void init() {
        this.db = new ImpermanentGraphDatabase();
        this.executionEngine = new ExecutionEngine(this.db);
    }

    /**
     * Shuts down the {@link ImpermanentGraphDatabase}.
     */
    @After
    public void close() {
        this.db.shutdown();
    }

    /**
     * Creates 10 nodes and performs a count query. Count query returns 11
     * because of 10 new nodes and 1 root node.
     */
    @Test
    public void executeCount() {
        final Transaction tx = db.beginTx();
        try {
            for (int i = 0; i < 10; i++) {
                db.createNode();
            }
            tx.success();
        } catch (final Exception e) {
            tx.failure();
        } finally {
            tx.finish();
        }
        final String countStatement = 
                "START n=node(*) " +
                "RETURN count(n) as nodeCount";
        final ExecutionResult result = this.executionEngine.execute(countStatement);
        Assert.assertEquals(11l, result.columnAs("nodeCount").next());
    }

    /**
     * Traverses through some relationship types.
     */
    @Test
    public void traverse() {
        final Transaction tx = db.beginTx();
        long userId = 0l;
        try {
            final Node user = db.createNode();
            user.setProperty("name", "Timmy");
            userId = user.getId();
            final Node database = db.createNode();
            database.setProperty("name", "Neo4j");
            user.createRelationshipTo(database, Relationships.LIKES);
            final Node language = db.createNode();
            language.setProperty("name", "Cypher");
            database.createRelationshipTo(language, Relationships.USES);
            tx.success();
        } catch (final Exception e) {
            tx.failure();
        } finally {
            tx.finish();
        }
        final String countStatement = 
                "START u=node(" + userId + ") " +
                "MATCH u-[:LIKES]-db-[:USES]-l " +
                "RETURN u.name as userName, db.name as dbName, l.name as languageName";
        final ExecutionResult result = this.executionEngine.execute(countStatement);
        final Map<String,Object> resultMap = result.iterator().next();
        Assert.assertEquals("Timmy", resultMap.get("userName"));
        Assert.assertEquals("Neo4j", resultMap.get("dbName"));
        Assert.assertEquals("Cypher", resultMap.get("languageName"));
    }

    static enum Relationships implements RelationshipType {
        LIKES, USES;
    }

}
