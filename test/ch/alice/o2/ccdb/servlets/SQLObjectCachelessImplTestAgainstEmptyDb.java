package ch.alice.o2.ccdb.servlets;

import org.junit.jupiter.api.BeforeAll;

class SQLObjectCachelessImplTestAgainstEmptyDb extends SQLObjectTestAgainstEmptyDb {
    @BeforeAll
    static void setToMultiMasterVersion() {
        SQLObject.multiMasterVersion = true;
    }
}