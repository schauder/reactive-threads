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
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jens Schauder
 */
public class InfinityFlux {

    private AtomicInteger counter = new AtomicInteger();

    public final Flux<Integer> tokens;
    private final BlockingQueue<Integer> returnedTokens;


    InfinityFlux(int maximumLoad) {

        returnedTokens = new LinkedBlockingQueue<>(maximumLoad );

        tokens = Flux.range(0, maximumLoad)

                .concatWith(Flux.create(fs -> {
                    while (!fs.isCancelled()) {
                        try {
                            System.out.print("w:");

                            Integer take = returnedTokens.take();
                            fs.next(take);
                            System.out.print("t:" + take);
                        } catch (InterruptedException e) {
                            fs.error(e);
                        }
                    }
                    System.out.println("canceled");
                }))
                .doOnNext(System.out::print)
                .cache(0);
    }

    public void returnToken() {
        try {
            int c = counter.incrementAndGet();
            returnedTokens.put(c);
            System.out.println("r:" + c);
        } catch (InterruptedException e) {
            throw new RuntimeException("failed to increment counter", e);
        }
    }

    public static void main(String[] args) {

        InfinityFlux infinityFlux = new InfinityFlux(10);
        Flux<Integer> tokens = infinityFlux.tokens;

        Flux.range(0, 1000)
                .publishOn(Schedulers.newElastic("blah"))
                .delayElements(Duration.ofMillis(100))
                .map(Object::toString)
                .map(s -> s + " ")
                .flatMap(s -> tokens.next().doOnTerminate(() -> infinityFlux.returnToken()).map(i -> s + " " + i))
                .doOnNext(System.out::println)
                .blockLast(Duration.ofSeconds(30));
    }

}
