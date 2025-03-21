package sqlancer.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import sqlancer.Main;

public class TestQuestDBTLP {
    @Test
    public void testQuestDB() {
        assertEquals(0,Main.executeMain(new String[] {
            "--random-seed", "0",
            "--timeout-seconds", TestConfig.SECONDS,
            "--num-queries", TestConfig.NUM_QUERIES,
            "--num-threads", "1",     
            "questdb"
        }));
    }
    
}
