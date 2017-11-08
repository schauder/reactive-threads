package de.schauder.reactivethreads.demo;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class DemoApplication {

    public static void main(String[] args) {
        new DemoApplication().run();

    }

    private void run() {

        Flux
                .range(1, 10)
                .publishOn(Schedulers.newElastic("blocking"))
                .map(this::blocking)
                .publishOn(Schedulers.newElastic("cpu"))
                .map(this::cpuBound)
                .blockLast();
    }

    private Integer cpuBound(Integer integer) {

        log(integer, "cpubound");
        return integer;
    }

    private Integer blocking(Integer integer) {

        log(integer, "blocking");
        return integer;
    }

    private void log(Integer integer, String label) {
        System.out.println(String.format("%s\t%d\t%s", label, integer, Thread.currentThread().getName()));
    }
}
