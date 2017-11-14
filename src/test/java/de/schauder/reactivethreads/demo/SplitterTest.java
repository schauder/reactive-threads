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

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Jens Schauder
 * @author Gerrit Meier
 */
public class SplitterTest {

    @Test
    public void eachEventGetsPushedOnce() {

        Flux<Integer> range = Flux.range(1, 10);

        Mono<Integer> splitted = split(range);


        StepVerifier.create(splitted).expectNext(1).expectComplete().verify();
        StepVerifier.create(splitted).expectNext(2).expectComplete().verify();
        StepVerifier.create(splitted).expectNext(3).expectComplete().verify();
    }

    @Test
    public void eachEventGetsPushedOnceDelayed() {

        Flux<Integer> range = Flux.range(1, 10).delayElements(Duration.ofMillis(20));

        Mono<Integer> splitted = split(range);

        StepVerifier.create(splitted).expectNext(1).expectComplete().verify();
        StepVerifier.create(splitted).expectNext(2).expectComplete().verify();
        StepVerifier.create(splitted).expectNext(3).expectComplete().verify();
    }

    private <T> Mono<T> split(Flux<T> flux) {

        return new SplittingMono<>(flux);
    }

    private static class SplittingMono<T> extends Mono<T> {

        private final Flux<T> flux;
        public boolean subscribed;
        Queue<CoreSubscriber> subscribers;

        public void setUpstream(Subscription upstream) {
            this.upstream = upstream;
        }

        Subscription upstream;

        public SplittingMono(Flux<T> flux) {
            this.flux = flux;
            subscribers = new LinkedList<>();
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> subscriber) {

            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long l) {
                    // TODO same subscriber subscribing multiple times
                    // TODO same subscriber requesting multiple times
                    subscribers.add(subscriber);
                    if (upstream != null) {
                        upstream.request(1);
                    }
                }

                @Override
                public void cancel() {

                }
            });

            subscribeUpstream();
        }


        private void subscribeUpstream() {

            if (!subscribed) {
                subscribed = true;

                flux.subscribe(new Subscriber<T>() {
                    @Override
                    public void onSubscribe(Subscription subscription) {
                        setUpstream(subscription);
                        subscription.request(subscribers.size());
                    }

                    @Override
                    public void onNext(T t) {
                        CoreSubscriber downStream = subscribers.remove();
                        downStream.onNext(t);
                        downStream.onComplete();

                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });
            }
        }
    }
}
