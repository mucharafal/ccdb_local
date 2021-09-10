package ch.alice.o2.ccdb.servlets;

import ch.alice.o2.ccdb.RequestParser;
import lazyj.DBFunctions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author rmucha
 * @since 2021-08-09
 */
public class SQLObjectTestAgainstFilledDb implements FilledDatabaseForTest {

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

    void saveInDatabase(SQLObject object, int expectedNumberOfObjects, int expectedNumberOfPaths) {
        object.save(null);

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("select count(1) from ccdb;");
            db.moveNext();
            Integer numberOfObjects = Integer.valueOf(db.geti(1));
            assertEquals(expectedNumberOfObjects, numberOfObjects);
        }

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("select count(1) from ccdb_paths;");
            db.moveNext();
            Integer numberOfObjects = Integer.valueOf(db.geti(1));
            assertEquals(expectedNumberOfPaths, numberOfObjects);
        }

        SQLObject objectReadFromDb = SQLObject.getObject(object.id);
        assertAreSQLObjectsEqual(object, objectReadFromDb);
    }

    @Test
    void insertToDatabase() {
        SQLObject object = SQLObject.fromPath("path");
        object.setContentType("x");
        object.md5 = UUID.randomUUID().toString();
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

    /**
     * @param first
     * @param second
     * @return `true` if everything works as expected
     */
    public static void assertAreSQLObjectsEqual(SQLObject first, SQLObject second) {
        assertEquals(first, second);
        assertEquals(first.id, second.id);
        assertEquals(first.getPath(), second.getPath());
        assertEquals(first.getContentType(), second.getContentType());
        assertEquals(first.getMetadataKeyValue(), second.getMetadataKeyValue());
        assertEquals(first.createTime, second.createTime);
        assertEquals(first.md5.replace("-", ""), second.md5); // todo is it intended? "1111-222-111" to "1111222111"
    }



}
