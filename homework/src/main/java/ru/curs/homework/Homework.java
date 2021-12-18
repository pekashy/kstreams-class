package ru.curs.homework;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class Homework {
    /*
    Homework() { // uncomment this in case of 'No visible constructors in class ru.curs.homework.Homework' exception
    }
    */

    private Homework() {
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(Homework.class).headless(false).run(args);
    }

}
