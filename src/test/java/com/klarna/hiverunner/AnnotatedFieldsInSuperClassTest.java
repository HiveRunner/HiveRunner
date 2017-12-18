package com.klarna.hiverunner;

import org.junit.Test;

public class AnnotatedFieldsInSuperClassTest extends AnnotatedBaseTestClass {
    @Test
    public void testShellInitializedInAbstractTestClass() {
        shell.executeQuery("select * from test_db.test_table");
    }
}

