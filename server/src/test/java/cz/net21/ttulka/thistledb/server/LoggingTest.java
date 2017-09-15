package cz.net21.ttulka.thistledb.server;

import lombok.extern.apachecommons.CommonsLog;
import org.junit.Test;

@CommonsLog
public class LoggingTest {

    @Test
    public void test() {
        log.debug("My debug");
        log.info("My info");
        log.warn("My warn");
        log.error("My error");
    }
}
