package ch.alice.o2.ccdb.servlets;

import org.junit.jupiter.api.BeforeAll;

class SQLObjectCachelessImplTest extends SQLObjectTest {
    @BeforeAll
    static void setToMultiMasterVersion() {
        SQLObject.multiMasterVersion = true;
    }
}