package ch.alice.o2.ccdb.servlets;

import ch.alice.o2.ccdb.RequestParser;
import lazyj.DBFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class SQLObjectTest {

    @BeforeEach
    void setUp() {
        try (DBFunctions db = SQLObject.getDB()) {
            db.query("insert into ccdb_paths values (100, 'x');\n" +
                    "insert into ccdb_paths values (101, 'a');\n" +
                    "insert into ccdb_contenttype values (100, 'x');\n" +
                    "insert into ccdb_contenttype values (101, 'a');\n" +
                    "insert into ccdb_metadata values (100, 'x');\n" +
                    "insert into ccdb_metadata values (101, 'a');\n" +
                    "insert into ccdb values ('a3fe6ab0-82a0-11eb-8f02-08f1eaf0250c', 100, '[1970-01-02 20:26:40,1970-01-02 20:36:40)', 1615491202269, '{1}', 100000, '7e8fbee4-f76f-7079-ec87-bdc83d7d5538', 'filename', 100, '127.0.0.1', 160000000, '100 => \\\"y\\\"', 160000001);\n" +
                    "insert into ccdb values ('a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c', 100, '[1970-01-02 20:26:41,1970-01-02 20:36:41)', 1615491202270, '{1}', 100000, '7e8fbee4-f76f-7079-ec87-bdc83d7d5538', 'filename', 100, '127.0.0.1', 160000000, '100 => \\\"y\\\"', 160000001);\n" +
                    "insert into ccdb values ('a3fe6ab0-82a0-11eb-8f02-08f1eaf0252c', 101, '[1970-01-02 20:26:42,1970-01-02 20:36:42)', 1615491202271, '{1}', 100000, '7e8fbee4-f76f-7079-ec87-bdc83d7d5538', 'filename', 101, '127.0.0.1', 160000000, '101 => \\\"y\\\"', 160000001);\n" +
                    "insert into ccdb values ('a3fe6ab0-82a0-11eb-8f02-08f1eaf0253c', 101, '[1970-01-02 20:26:43,1970-01-02 20:36:43)', 1615491202272, '{1}', 100000, '7e8fbee4-f76f-7079-ec87-bdc83d7d5538', 'filename', 101, '127.0.0.1', 160000000, '101 => \\\"y\\\"', 160000001);\n"
            );
        }
    }

    @AfterEach
    void tearDown() {
        try (DBFunctions db = SQLObject.getDB()) {
            db.query("delete from ccdb where pathid < 1000 and pathid > 10;\n" +
                    "delete from ccdb_paths where path in ('a', 'b', 'x', 'y');\n" +
                    "delete from ccdb_metadata where metadataKey in ('a', 'b', 'x', 'y');\n" +
                    "delete from ccdb_contenttype where contentType in ('a', 'b', 'x', 'y');"
            );
        }
    }

    @Test
    void getObjectsFromDatabaseByID() {
        SQLObject object1 = SQLObject.getObject(UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0250c"));
        SQLObject object2 = SQLObject.getObject(UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"));
        SQLObject object3 = SQLObject.getObject(UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0252c"));
        SQLObject object4 = SQLObject.getObject(UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0253c"));

        assertNotNull(object1);
        assertNotNull(object2);
        assertNotNull(object3);
        assertNotNull(object4);
    }

    @Test
    void getMatchingObjectFromDatabase() {
        RequestParser parser = new RequestParserImpl("x");
        SQLObject object1 = SQLObject.getMatchingObject(parser);

        assertEquals(UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"), object1.id);
    }

    @Test
    void getMatchingObjectsFromDatabaseUsingRegex() {
        RequestParser parser = new RequestParserImpl(".*");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        assertEquals(2, objects.size());
        SQLObject[] arrayOfObjects = objects.toArray(SQLObject[]::new);
        assertEquals(arrayOfObjects[0].id, UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"));
        assertEquals(arrayOfObjects[1].id, UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0253c"));
    }

    @Test
    void getMatchingObjectsFromDatabaseUsingPostgreSQLMatching() {
        RequestParser parser = new RequestParserImpl("%");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        assertEquals(2, objects.size());
        SQLObject[] arrayOfObjects = objects.toArray(SQLObject[]::new);
        assertEquals(arrayOfObjects[0].id, UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"));
        assertEquals(arrayOfObjects[1].id, UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0253c"));
    }

    @Test
    void getMatchingObjectsUsingPostgreSQLMatchingWhichPathStartsWithX() {
        RequestParser parser = new RequestParserImpl("x%");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        assertEquals(1, objects.size());
        SQLObject[] arrayOfObjects = objects.toArray(SQLObject[]::new);
        assertEquals(arrayOfObjects[0].id, UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"));
    }

    @Test
    void getMatchingObjectsUsingRegexWhichPathStartsWithX() {
        RequestParser parser = new RequestParserImpl("x.*");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        assertEquals(1, objects.size());
        SQLObject[] arrayOfObjects = objects.toArray(SQLObject[]::new);
        assertEquals(arrayOfObjects[0].id, UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"));
    }




}
