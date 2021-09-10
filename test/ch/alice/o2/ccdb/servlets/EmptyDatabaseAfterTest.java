package ch.alice.o2.ccdb.servlets;

import lazyj.DBFunctions;
import org.junit.jupiter.api.AfterEach;

public interface EmptyDatabaseAfterTest {

    @AfterEach
    static void tearDown() {
        try (DBFunctions db = SQLObject.getDB()) {
            db.query("delete from ccdb where pathid in (select pathid from ccdb_paths where path in ('a', 'b', 'x', 'y', 'path'));\n" +
                    "delete from ccdb_paths where path in ('a', 'b', 'x', 'y', 'path');\n" +
                    "delete from ccdb_metadata where metadataKey in ('a', 'b', 'x', 'y');\n" +
                    "delete from ccdb_contenttype where contentType in ('a', 'b', 'x', 'y');"
            );
        }
        SQLObject.clearCaches();
    }
}
