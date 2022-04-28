package de.mel.lok;

import de.mel.Lok;
import de.mel.LokImpl;
import org.junit.Test;

public class LokTest {
    @Test
    public void linesTest() {
        Lok.setLokImpl(new LokImpl().setup(2,true));
        Lok.debug("test1");
        Lok.debug("test2");
        Lok.debug("test3");
        Lok.debug("test4");
        String[] lines = Lok.getLines();
        System.out.println("LokTest.linesTest");
    }
}
