# reactive-threads
Playground for experimenting with Project Reactor and Threads

## note worthy stuff

### Backpressure for Resources not supporting Backpressure

https://github.com/schauder/reactive-threads/tree/master/src/main/java/de/schauder/reactivethreads/limited

The idea is to have a special `Producer` (`MonoSplitter`) that hands out tokens, but every token only once to a single `Subscriber`. 
If you wait with processing your `Flux` until you get a token, put a token back once you are done you should be able to limit the load to a value determined by the initial number of tokens, a demonstrated in `LimitedResource`.

_Please note: this is only an experiment with which I'm trying to find out if an approach like this is actually going to work.
Use at own risk and don't ask for Maven Central coordinates._
