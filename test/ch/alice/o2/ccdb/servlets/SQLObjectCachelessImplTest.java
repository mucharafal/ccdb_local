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

        try(DBFunctions db = SQLObject.getDB()) {
            db.query("update ccdb_path set pathid = 30 where path = 'x';");
        }

        saveInDatabase(toInsert, 5, 2);
    }
}