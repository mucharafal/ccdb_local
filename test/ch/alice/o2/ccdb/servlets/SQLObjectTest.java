package ch.alice.o2.ccdb.servlets;

import ch.alice.o2.ccdb.RequestParser;
import lazyj.DBFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class SQLObjectTest {
    protected static final UUID object1Id = UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c");
    protected static final UUID object3Id = UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0253c");

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
            db.query("delete from ccdb where pathid in (select pathid from ccdb_paths where path in ('a', 'b', 'x', 'y', 'path'));\n" +
                    "delete from ccdb_paths where path in ('a', 'b', 'x', 'y', 'path');\n" +
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

        assertEquals(object1Id, object1.id);
    }

    void checkObjects(Collection<SQLObject> objects) {
        assertEquals(2, objects.size());
        SQLObject[] arrayOfObjects = objects.toArray(SQLObject[]::new);
        if(object1Id.equals(arrayOfObjects[0].id)) {
            assertEquals(object1Id, arrayOfObjects[0].id);
            assertEquals(object3Id, arrayOfObjects[1].id);
        } else {
            assertEquals(object3Id, arrayOfObjects[0].id);
            assertEquals(object1Id, arrayOfObjects[1].id);
        }
    }

    @Test
    void getMatchingObjectsFromDatabaseUsingRegex() {
        RequestParser parser = new RequestParserImpl(".*");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        checkObjects(objects);
    }

    @Test
    void getMatchingObjectsFromDatabaseUsingPostgreSQLMatching() {
        RequestParser parser = new RequestParserImpl("%");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        checkObjects(objects);
    }

    @Test
    void getMatchingObjectsUsingPostgreSQLMatchingWhichPathStartsWithX() {
        RequestParser parser = new RequestParserImpl("x%");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        assertEquals(1, objects.size());
        SQLObject[] arrayOfObjects = objects.toArray(SQLObject[]::new);
        assertEquals(UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"), arrayOfObjects[0].id);
    }

    @Test
    void getMatchingObjectsUsingRegexWhichPathStartsWithX() {
        RequestParser parser = new RequestParserImpl("x.*");
        Collection<SQLObject> objects = SQLObject.getAllMatchingObjects(parser);

        assertEquals(1, objects.size());
        SQLObject[] arrayOfObjects = objects.toArray(SQLObject[]::new);
        assertEquals(UUID.fromString("a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c"), arrayOfObjects[0].id);
    }

    protected void saveInDatabase(SQLObject object, int expectedNumberOfObjects, int expectedNumberOfPaths) {
        object.save(null);

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("select count(1) from ccdb;");
            db.moveNext();
            Integer numberOfObjects = db.geti(1);
            assertEquals(expectedNumberOfObjects, numberOfObjects);
        }

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("select count(1) from ccdb_paths;");
            db.moveNext();
            Integer numberOfObjects = db.geti(1);
            assertEquals(expectedNumberOfPaths, numberOfObjects);
        }

        SQLObject objectReadFromDb = SQLObject.getObject(object.id);
        assertTrue(areSQLObjectsEqual(object, objectReadFromDb));
    }

    @Test
    void insertToDatabase() {
        SQLObject object = SQLObject.fromPath("path");
        object.setContentType("x");
        object.md5 = "md5";
        saveInDatabase(object, 5, 3);
    }

    @Test
    void updateContentType() {  // todo It is possible to update only a few fields in SQLObjects, can be confusing
        SQLObject object = SQLObject.getObject(object1Id);
        object.setContentType("a");
        object.tainted = true;
        saveInDatabase(object, 4, 2);
        assertEquals("a", SQLObject.getObject(object.id).getContentType());
    }

    public static boolean areSQLObjectsEqual(SQLObject first, SQLObject second) {
        return first.equals(second) &&
                first.id.equals(second.id) &&
                first.getPath().equals(second.getPath()) &&
                (first.getContentType() == second.getContentType() || first.getContentType().equals(second.getContentType())) &&
                first.getMetadataKeyValue().equals(second.getMetadataKeyValue()) &&
                first.getLastModified() == second.getLastModified() &&
                first.createTime == second.createTime &&
                first.md5.equals(second.md5);
    }


}
