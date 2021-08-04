package ch.alice.o2.ccdb.servlets;

import lazyj.DBFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;

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
                    "insert into ccdb values ('a3fe6ab0-82a0-11eb-8f02-08f1eaf0251c', 100, '[1970-01-02 20:26:40,1970-01-02 20:36:40)', 1615491202269, '{1}', 100000, '7e8fbee4-f76f-7079-ec87-bdc83d7d5538', 'filename', 100, '127.0.0.1', 160000000, '100 => \\\"y\\\"', 160000001);\n" +
                    "insert into ccdb values ('a3fe6ab0-82a0-11eb-8f02-08f1eaf0252c', 101, '[1970-01-02 20:26:40,1970-01-02 20:36:40)', 1615491202269, '{1}', 100000, '7e8fbee4-f76f-7079-ec87-bdc83d7d5538', 'filename', 101, '127.0.0.1', 160000000, '101 => \\\"y\\\"', 160000001);\n" +
                    "insert into ccdb values ('a3fe6ab0-82a0-11eb-8f02-08f1eaf0253c', 101, '[1970-01-02 20:26:40,1970-01-02 20:36:40)', 1615491202269, '{1}', 100000, '7e8fbee4-f76f-7079-ec87-bdc83d7d5538', 'filename', 101, '127.0.0.1', 160000000, '101 => \\\"y\\\"', 160000001);\n"
            );
        }
    }

    @AfterEach
    void tearDown() {
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
}
