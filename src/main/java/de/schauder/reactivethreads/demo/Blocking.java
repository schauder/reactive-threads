/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.schauder.reactivethreads.demo;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Jens Schauder
 */
public class Blocking {

    static AtomicInteger blocking = new AtomicInteger();
    static AtomicInteger working = new AtomicInteger();

    public static void main(String[] args) {

        startLogger();

        AtomicInteger event = new AtomicInteger();
        Scheduler cpu = Schedulers.newParallel("cpu", 4);
        Scheduler db = Schedulers.newParallel("db", 20);

        Flux
                .<Integer>generate(s -> s.next(event.incrementAndGet()))
                .publishOn(cpu)
                .doOnNext(__ -> work(10000))
                .publishOn(db)
                .doOnNext(__ -> block(100))
                .publishOn(cpu)
                .doOnNext(__ -> work(10000))
                .doOnNext(__ -> System.out.print('.'))
                .blockLast();

        cpu.dispose();
        db.dispose();
    }

    private static void startLogger() {

        long start = System.currentTimeMillis();
        new Thread(() -> {
            while (true) {
                System.out.println(String.format(
                        "\nAt %d \tworking: %d blocking %d",
                        System.currentTimeMillis() - start,
                        working.get(), blocking.get()
                ));
                safeSleep(1000);
            }
        }).start();
    }

    private static void block(int millis) {
        blocking.incrementAndGet();
        verifyOn("db");
        safeSleep(millis);
        blocking.decrementAndGet();
    }

    private static void work(int iterations) {
        working.incrementAndGet();
        verifyOn("cpu");
        for (int i = 0; i < iterations; i++) {
            new Random().nextInt();
        }
        working.decrementAndGet();
    }

    private static void safeSleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Consumer<Integer> log(String label) {
        return i -> System.out.println(label + " " + i);
    }


    private static void verifyOn(String name) {
        if (!Thread.currentThread().getName().contains(name)) {
            throw new IllegalStateException(String.format("Not on the expected thread '%s', but on '%s'", name, Thread.currentThread().getName()));
        }
    }
}
