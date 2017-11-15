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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jens Schauder
 */
class SplittingMono<T> extends Mono<T> {

    static <T> Mono<T> split(Publisher<T> publisher) {
        return new SplittingMono<>(publisher);
    }


    private final Publisher<T> publisher;
    private AtomicBoolean subscribed = new AtomicBoolean(false);
    private Queue<CoreSubscriber<? super T>> subscribers;

    private AtomicBoolean complete = new AtomicBoolean(false);
    private AtomicReference<Throwable> error = new AtomicReference<>(null);

    private void setUpstream(Subscription upstream) {
        this.upstream = upstream;
    }

    private Subscription upstream;

    private SplittingMono(Publisher<T> publisher) {
        this.publisher = publisher;
        subscribers = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> subscriber) {

        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long amount) {
// TODO: requesting multiple times.
                if (amount <= 0) {
                    subscriber.onError(new IllegalArgumentException("Argument of 'request' must be positive. See Reactive Streams Spec §3.9"));
                    return;
                }

                subscribers.add(subscriber);
                if (upstream != null) {
                    upstream.request(1);
                }
            }

            @Override
            public void cancel() {
// TODO
            }
        });

        subscribeUpstream();

        if (error.get() != null) {
            subscriber.onError(error.get());
        } else if (complete.get()) {
            subscriber.onComplete();
        }
    }


    private void subscribeUpstream() {

        if (subscribed.compareAndSet(false, true)) {

            publisher.subscribe(new Subscriber<T>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    setUpstream(subscription);
                    int size = subscribers.size();
                    if (size > 0) {
                        subscription.request(size);
                    }
                }

                @Override
                public void onNext(T t) {
                    CoreSubscriber<? super T> downStream = subscribers.remove();
                    downStream.onNext(t);
                    downStream.onComplete();

                }

                @Override
                public void onError(Throwable throwable) {
                    if (error.compareAndSet(null, throwable)) {
                        subscribers.forEach(s -> s.onError(error.get()));
                    }
                }

                @Override
                public void onComplete() {
                    if (complete.compareAndSet(false, true)) {
                        subscribers.forEach(Subscriber::onComplete);
                    }
                }
            });
        }
    }
}
