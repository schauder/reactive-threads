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

import org.junit.Test;
import org.mockito.InOrder;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static de.schauder.reactivethreads.limited.SplittingMono.split;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Jens Schauder
 * @author Gerrit Meier
 */
public class SplitterTest {

    static final Duration SHORT_WAIT = Duration.ofMillis(100);

    @Test
    public void eachEventGetsPushedOnce() {

        Flux<Integer> range = Flux.range(1, 10);

        Mono<Integer> splitted = split(range);


        StepVerifier.create(splitted).expectNext(1).expectComplete().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectNext(2).expectComplete().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectNext(3).expectComplete().verify(SHORT_WAIT);
    }

    @Test
    public void complete() {

        Flux<Integer> range = Flux.just(1);

        Mono<Integer> splitted = split(range);


        StepVerifier.create(splitted).expectNext(1).expectComplete().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectComplete().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectComplete().verify(SHORT_WAIT);
    }

    @Test
    public void eventualError() {

        Flux<Integer> range = Flux.just(1).concatWith(Mono.error(new RuntimeException()));

        Mono<Integer> splitted = split(range);

        StepVerifier.create(splitted).expectNext(1).expectComplete().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectError().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectError().verify(SHORT_WAIT);
    }

    @Test
    public void immediateError() {

        Flux<Integer> range = Flux.error(new RuntimeException());

        Mono<Integer> splitted = split(range);

        StepVerifier.create(splitted).expectError().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectError().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectError().verify(SHORT_WAIT);
    }

    @Test
    public void eachEventGetsPushedOnceDelayed() {

        Flux<Integer> range = Flux.range(1, 10).delayElements(Duration.ofMillis(20));

        Mono<Integer> splitted = split(range);

        StepVerifier.create(splitted).expectNext(1).expectComplete().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectNext(2).expectComplete().verify(SHORT_WAIT);
        StepVerifier.create(splitted).expectNext(3).expectComplete().verify(SHORT_WAIT);
    }

    @Test
    public void sendsCompleteWithOutRequest() {

        TestPublisher<Object> publisher = TestPublisher.create();

        Mono<Object> splitted = SplittingMono.split(publisher);

        CoreSubscriber subscriber = mock(CoreSubscriber.class);

        splitted.subscribe(subscriber);

        publisher.complete();

        InOrder inOrder = inOrder(subscriber);
        inOrder.verify(subscriber).onSubscribe(any(Subscription.class));
        inOrder.verify(subscriber).onComplete();
        verifyNoMoreInteractions(subscriber);
    }

    @Test
    public void honorsCancel() {

        TestPublisher<Object> publisher = TestPublisher.create();

        Mono<Object> splitted = SplittingMono.split(publisher);

        AtomicReference<Subscription> subscription = new AtomicReference<>();

        CoreSubscriber subscriber = spy(new CoreSubscriber() {

            @Override
            public void onSubscribe(Subscription s) {
                subscription.set(s);
            }

            @Override
            public void onNext(Object o) {
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        });

        splitted.subscribe(subscriber);

        assertThat(subscription.get()).isNotNull();

        subscription.get().cancel();

        publisher.next("event-shouldn't make it through");

        verify(subscriber).onSubscribe(any(Subscription.class));
        verifyNoMoreInteractions(subscriber);
    }


}
