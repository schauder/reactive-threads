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
package de.schauder.reactivethreads.limited;

import lombok.Value;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static de.schauder.reactivethreads.limited.SplittingMono.PredefinedState.CANCELED;
import static de.schauder.reactivethreads.limited.SplittingMono.PredefinedState.NOT_SUBSCRIBED;

/**
 * @author Jens Schauder
 */
class SplittingMono<T> extends Mono<T> {

    static <T> Mono<T> split(Publisher<T> publisher) {
        return new SplittingMono<>(publisher);
    }


    private AtomicReference<State> state = new AtomicReference<>(NOT_SUBSCRIBED);

    private final Publisher<T> publisher;
    private Set<CoreSubscriber<? super T>> subscribers = ConcurrentHashMap.newKeySet();


    private SplittingMono(Publisher<T> publisher) {
        this.publisher = publisher;

    }

    @Override
    public void subscribe(CoreSubscriber<? super T> downstream) {


        subscribeUpstream();

        subscribers.add(downstream);
        System.out.println("got subscription");

        state.get().subscribe(downstream);
    }


    private void subscribeUpstream() {

        AtomicReference<Subscription> upstream = new AtomicReference<>();
        Subscribed<T> thisState = new Subscribed<>(upstream);

        if (state.compareAndSet(NOT_SUBSCRIBED, thisState)) {

            System.out.println("subscribing upstream");
            publisher.subscribe(new Subscriber<T>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    System.out.println("on subscription");
                    upstream.set(subscription);
                    int size = thisState.requesters.size();
                    if (size > 0) {
                        subscription.request(size);
                    }
                }

                @Override
                public void onNext(T t) {
                    CoreSubscriber<? super T> downStream = thisState.requesters.remove();
                    subscribers.remove(downStream);
                    downStream.onNext(t);
                    downStream.onComplete();
                }

                @Override
                public void onError(Throwable throwable) {
                    if (state.compareAndSet(thisState, new Errored(throwable))) {
                        subscribers.forEach(s -> s.onError(throwable));
                    }
                }

                @Override
                public void onComplete() {
                    if (state.compareAndSet(thisState, CANCELED)) {
                        subscribers.forEach(Subscriber::onComplete);
                    }
                }
            });
        }
    }

    interface State<T> {
        @Deprecated
        Subscription createSubscription(Subscriber<? super T> downstream);

        void subscribe(Subscriber<? super T> downstream);
    }

    enum PredefinedState implements State<Object> {
        NOT_SUBSCRIBED {
            @Override
            public Subscription createSubscription(Subscriber<? super Object> downstream) {
                throw new UnsupportedOperationException("Can't create a subscription before being subscribed");
            }

            @Override
            public void subscribe(Subscriber<? super Object> downstream) {
                throw new UnsupportedOperationException("Can't subscribe before being subscribed myself");
            }
        },
        CANCELED {
            @Override
            public Subscription createSubscription(Subscriber<? super Object> downstream) {
                return new CompletedSubscription();
            }

            @Override
            public void subscribe(Subscriber<? super Object> downstream) {
                downstream.onSubscribe(createSubscription(downstream));
                downstream.onComplete();
            }
        }
    }

    static class Subscribed<T> implements State<T> {

        private final AtomicReference<Subscription> upstream;
        private final Queue<CoreSubscriber<? super T>> requesters = new ConcurrentLinkedQueue<>();

        Subscribed(AtomicReference<Subscription> upstream) {
            this.upstream = upstream;
        }


        @Override
        public Subscription createSubscription(Subscriber<? super T> downstream) {
            return new Subscription() {
                @Override
                public void request(long amount) {
                    System.out.println("got request");
// TODO: requesting multiple times.
                    if (amount <= 0) {
                        downstream.onError(new IllegalArgumentException("Argument of 'request' must be positive. See Reactive Streams Spec §3.9"));
                        return;
                    }
// TODO: I think this needs a synchronized block
                    requesters.add((CoreSubscriber<? super T>) downstream);
                    Subscription subscription = upstream.get();
                    if (subscription != null) {
                        subscription.request(1);
                    }
                }

                @Override
                public void cancel() {
// TODO
                }
            };
        }

        @Override
        public void subscribe(Subscriber<? super T> downstream) {
            downstream.onSubscribe(createSubscription(downstream));
        }
    }


    @Value
    static class Errored implements State<Object> {

        Throwable error;

        @Override
        public Subscription createSubscription(Subscriber<? super Object> downstream) {
            return new CompletedSubscription();
        }

        @Override
        public void subscribe(Subscriber<? super Object> downstream) {
            downstream.onSubscribe(createSubscription(downstream));
            downstream.onError(error);
        }
    }

    private static class CompletedSubscription implements Subscription {
        @Override
        public void request(long l) {

        }

        @Override
        public void cancel() {

        }
    }
}

