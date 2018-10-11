package com.dmy.graduation.core;

import com.dmy.graduation.partitioner.Partitioner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(locations = "classpath*:/applicationTest.xml")
public class ApplicationTest {

    @Autowired
    private Partitioner partitioner;

    @Test
    public void contextLoads() {
        partitioner.display();
    }

}
