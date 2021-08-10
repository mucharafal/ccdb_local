package ch.alice.o2.ccdb.servlets;

import lazyj.DBFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author rmucha
 * @since 2021-08-09
 */
public class SQLObjectTestAgainstEmptyDb {

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
        saveInDatabase(object, 1, 1);
    }

    @Test
    void updateContentType() {  // todo It is possible to update only a few fields in SQLObjects, can be confusing
        SQLObject object = SQLObject.fromPath("path");
        object.setContentType("x");
        object.md5 = UUID.randomUUID().toString();
        saveInDatabase(object, 1, 1);
        object.setContentType("a");
        object.tainted = true;
        saveInDatabase(object, 1, 1);
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
