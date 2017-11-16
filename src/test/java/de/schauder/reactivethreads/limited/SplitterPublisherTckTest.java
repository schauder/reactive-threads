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
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.publisher.Flux;

/**
 * @author Jens Schauder
 */
public class SplitterPublisherTckTest extends PublisherVerification<Integer> {


    public SplitterPublisherTckTest() {
        super(new TestEnvironment(true));
    }

    @Override
    public Publisher<Integer> createPublisher(long l) {
        return SplittingMono.split(Flux.range(1, (int) l));
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return SplittingMono.split(Flux.error(new RuntimeException()));
    }

    @Override
    public long maxElementsFromPublisher() {
        return 1;
    }

    @Override
    public long boundedDepthOfOnNextAndRequestRecursion() {
        return 1;
    }
}
