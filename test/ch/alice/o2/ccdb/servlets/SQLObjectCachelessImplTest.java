package ch.alice.o2.ccdb.servlets;

import lazyj.DBFunctions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SQLObjectCachelessImplTest extends SQLObjectTest {
    @BeforeAll
    static void setToMultiMasterVersion() {
        SQLObject.multiMasterVersion = true;
    }

    @Test
    void insertAfterUpdatePath() {
        SQLObject objectWithPathX = SQLObject.getObject(object1Id);

        assertNotNull(objectWithPathX);

        SQLObject toInsert = SQLObject.fromPath("x");
        toInsert.setContentType("x");
        toInsert.md5 = UUID.randomUUID().toString();

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("update ccdb_paths set pathid = 30 where path = 'x';");
        }

        saveInDatabase(toInsert, 5, 2);
    }

    @Test
    void getObjectsAfterUpdatePath() {
        SQLObject objectWithPathA = SQLObject.getObject(object3Id);
        SQLObject objectWithPathX = SQLObject.getObject(object1Id);

        assertNotNull(objectWithPathA);
        assertNotNull(objectWithPathX);

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("update ccdb_paths set pathid = 30 where path = 'x';");
            db.query("update ccdb_paths set pathid = 100 where path = 'a';");
            db.query("update ccdb_paths set pathid = 101 where path = 'x';");
        }

        assertEquals("a", SQLObject.getObject(objectWithPathA.id).getPath());
        assertEquals("x", SQLObject.getObject(objectWithPathX.id).getPath());
    }

    @Test
    void updateAfterUpdateContenttypeIdInDatabase() {
        SQLObject objectWithContentTypeA = SQLObject.getObject(object3Id);

        assertNotNull(objectWithContentTypeA);

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("update ccdb_contenttype set contenttypeid = 30 where path = 'x';");
            db.query("update ccdb_contenttype set contenttypeid = 100 where path = 'a';");
            db.query("update ccdb_contenttype set contenttypeid = 101 where path = 'x';");
        }

        objectWithContentTypeA.setContentType("x");
        saveInDatabase(objectWithContentTypeA, 4, 2);

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("update ccdb_contenttype set contenttypeid = 30 where path = 'x';");
        }

        assertEquals("x", SQLObject.getObject(objectWithContentTypeA.id).getContentType());
    }
}