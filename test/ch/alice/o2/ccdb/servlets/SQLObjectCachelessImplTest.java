package ch.alice.o2.ccdb.servlets;

import lazyj.DBFunctions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
        toInsert.md5 = "md5";

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
}