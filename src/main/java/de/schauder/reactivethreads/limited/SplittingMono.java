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
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static de.schauder.reactivethreads.limited.SplittingMono.PredefinedState.COMPLETED;
import static de.schauder.reactivethreads.limited.SplittingMono.PredefinedState.NOT_SUBSCRIBED;

/**
 * @author Jens Schauder
 */
class SplittingMono<T> extends Mono<T> {

    static <T> Mono<T> split(Publisher<T> publisher) {
        return new SplittingMono<>(publisher);
    }


    @SuppressWarnings("unchecked")
    private AtomicReference<State<T>> state = new AtomicReference<>((State<T>) NOT_SUBSCRIBED);

    private final Publisher<T> publisher;


    private SplittingMono(Publisher<T> publisher) {
        this.publisher = publisher;

    }

    @Override
    public void subscribe(@Nullable CoreSubscriber<? super T> downstream) {

        if (downstream == null) return;
        subscribeUpstream();
        state.get().subscribe(downstream);
    }


    @SuppressWarnings("unchecked")
    private void subscribeUpstream() {

        final AtomicReference<Subscription> upstream = new AtomicReference<>();
        final Subscribed<T> newState = new Subscribed<>(upstream);

        if (state.compareAndSet((State) NOT_SUBSCRIBED, newState)) {

            publisher.subscribe(new Subscriber<T>() {
                @Override
                public void onSubscribe(Subscription subscription) {

                    synchronized (newState) {
                        upstream.set(subscription);
                        int size = newState.requesters.size();
                        if (size > 0) {
                            subscription.request(size);
                        }
                    }
                }

                @Override
                public void onNext(T t) {

                    Subscriber<? super T> downStream = newState.requesters.remove();
                    Assert.notNull(downStream, "We didn't get a requester for a call to onNext. Either we f***** up, or we get more calls to onNext then requested!");

                    newState.subscribers.remove(downStream);
                    downStream.onNext(t);
                    downStream.onComplete();
                }

                @Override
                public void onError(Throwable throwable) {
                    if (state.compareAndSet(newState, new Errored(throwable))) {
                        newState.subscribers.forEach(s -> s.onError(throwable));
                        newState.subscribers.clear();
                    }
                }

                @Override
                public void onComplete() {
                    if (state.compareAndSet(newState, (State<T>) COMPLETED)) {
                        newState.subscribers.forEach(Subscriber::onComplete);
                        newState.subscribers.clear();
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

        COMPLETED {
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
        private Set<Subscriber<? super T>> subscribers = ConcurrentHashMap.newKeySet();
        private final Queue<Subscriber<? super T>> requesters = new ConcurrentLinkedQueue<>();

        Subscribed(AtomicReference<Subscription> upstream) {
            this.upstream = upstream;
        }


        @Override
        public Subscription createSubscription(Subscriber<? super T> downstream) {

            return new Subscription() {

                @Override
                public void request(long amount) {

// TODO: requesting multiple times.
                    if (amount <= 0) {
                        downstream.onError(new IllegalArgumentException("Argument of 'request' must be positive. See Reactive Streams Spec §3.9"));
                        return;
                    }

                    synchronized (Subscribed.this) {
                        requesters.add(downstream);
                        Subscription subscription = upstream.get();
                        if (subscription != null) {
                            subscription.request(1);
                        }
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
            subscribers.add(downstream);
            downstream.onSubscribe(createSubscription(downstream));
        }
    }


    @Value
    static class Errored implements State {

        Throwable error;

        @Override
        public Subscription createSubscription(Subscriber downstream) {
            return new CompletedSubscription();
        }

        @Override
        public void subscribe(Subscriber downstream) {

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

