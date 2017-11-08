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
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jens Schauder
 */
public class FlatMap {

    public static void main(String[] args) {

        concatMap();
    }

    private static void simple() {

        long start = System.currentTimeMillis();
        AtomicInteger integer = new AtomicInteger();

        Flux
                .<Integer>generate(s -> {

                    int next = integer.incrementAndGet();
                    System.out.println(System.currentTimeMillis() - start + " - " + next);
                    s.next(next);
                })
                .take(10)
                .blockLast(Duration.ofSeconds(1));
    }

    private static void flatmap() {

        long start = System.currentTimeMillis();
        AtomicInteger integer = new AtomicInteger();

        Flux
                .<Integer>generate(s -> {

                    int next = integer.incrementAndGet();
                    System.out.println(System.currentTimeMillis() - start + " - " + next);
                    s.next(next);
                })
                .flatMap(Mono::just)
                .take(10)
                .blockLast(Duration.ofSeconds(1));
    }

    private static void delayed() {

        long start = System.currentTimeMillis();
        AtomicInteger integer = new AtomicInteger();

        Flux
                .<Integer>generate(s -> {

                    System.out.println(System.currentTimeMillis() - start + " - " + integer.get());
                    s.next(integer.incrementAndGet());
                })
                .flatMap(i -> Mono.just(i).delayElement(Duration.ofMillis(20)))
                .take(10)
                .blockLast(Duration.ofSeconds(1));
    }

    private static void controlled() {

        long start = System.currentTimeMillis();
        AtomicInteger integer = new AtomicInteger();

        Flux
                .<Integer>generate(s -> {

                    System.out.println(System.currentTimeMillis() - start + " - " + integer.get());
                    s.next(integer.incrementAndGet());
                })
                .flatMap(i -> Mono.just(i).delayElement(Duration.ofMillis(20)), 20)
                .take(10)
                .blockLast(Duration.ofSeconds(1));
    }

    private static void concatMap() {

        long start = System.currentTimeMillis();
        AtomicInteger integer = new AtomicInteger();

        Flux
                .<Integer>generate(s -> {

                    System.out.println(System.currentTimeMillis() - start + " - " + integer.get());
                    s.next(integer.incrementAndGet());
                })
                .concatMap(i -> Mono.just(i).delayElement(Duration.ofMillis(20)))
                .take(10)
                .blockLast(Duration.ofSeconds(1));
    }

    private static void sleepy() {

        long start = System.currentTimeMillis();
        AtomicInteger integer = new AtomicInteger();

        Flux
                .<Integer>generate(s -> {

                    System.out.println(System.currentTimeMillis() - start + " - " + integer.get());
                    s.next(integer.incrementAndGet());
                })
                .flatMap(i -> Mono.just(i).doOnNext(__ -> {try {Thread.sleep(200);} catch (InterruptedException e) { }}))
                .take(10)
                .blockLast(Duration.ofSeconds(1));
    }
}
