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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demo how one can protect a resource from overloading when it doesn't implement backpressure itself.
 *
 * @author Jens Schauder
 */
public class LimitedRessource {

    public static void main(String[] args) {
        new LimitedRessource().applyLoad();
    }

    // Resource resource = new OverloadableResource();
    Resource resource = new LoadLimitingResource( new OverloadableResource(), 3);

    Runnable load = () -> resource.doSomethingFluxish().blockLast();

    private void applyLoad() {

        for (int i = 0; i < 9; i++) {
            new Thread(() -> {
                while (true) {
                    load.run();
                    safeSleep(10);
                }
            }).start();
            safeSleep(100);
        }

    }

    private static void safeSleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private interface Resource {

        Flux<String> doSomethingFluxish();
    }


    /**
     * limits the load on a Resource per instance basis.
     */
    private static class LoadLimitingResource implements Resource {
        AtomicInteger counter = new AtomicInteger();

        private final Flux<Integer> tokens;
        private final BlockingQueue<Integer> returnedTokens;


        private final Resource delegate;

        LoadLimitingResource(Resource delegate, int maximumLoad) {

            this.delegate = delegate;
            returnedTokens = new LinkedBlockingQueue<>(maximumLoad+1);

            tokens = Flux.range(0, maximumLoad)
                    .concatWith(Flux.create(fs -> {
                        while (!fs.isCancelled()) {
                            try {
                                Integer take = returnedTokens.take();
                                fs.next(take);
                                System.out.print("t:" + take);
                            } catch (InterruptedException e) {
                                fs.error(e);
                            }
                        }
                    }));
        }


        @Override
        public Flux<String> doSomethingFluxish() {
            return tokens.next().thenMany(delegate.doSomethingFluxish().doOnTerminate(() -> {
                int counter = this.counter.incrementAndGet();
                returnedTokens.add(counter);
                System.out.print("p:" + counter);
            }));
        }
    }


    /**
     * stand in for a resource which might get overloaded. Maybe a database connection which might kill the database when used too heavily.
     * <p>
     * Or even which will degrade in performance.
     */
    private static class OverloadableResource implements Resource {
        private static final int LOAD_LIMIT = 6;
        static AtomicInteger load = new AtomicInteger();

        public Flux<String> doSomethingFluxish() {

            addLoad();
            return Flux.just("flux-result")
                    .doOnTerminate(() -> load.decrementAndGet());
        }


        private int addLoad() {
            int currentLoad = OverloadableResource.load.incrementAndGet();
            System.out.print(currentLoad + " ");
            if (currentLoad > LOAD_LIMIT) {
                OverloadableResource.load.decrementAndGet();
                throw new IllegalStateException(String.format("too much load! (%s)", currentLoad));
            }
            return currentLoad;
        }
    }

}
