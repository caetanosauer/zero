#include "ringbuffer.h"
#include "w_debug.h"

#include <cstdlib>

const char * rundir = "/tmp/archiver_test";
size_t bsize = 8192;
size_t bcount = 16;
int produce_count = 100;
AsyncRingBuffer * buf = new AsyncRingBuffer(bsize, bcount);

class producer_t : public smthread_t {
    void run() {
        for (int i = 0; i < produce_count; i++) {
            char* b = buf->producerRequest();
            uint32_t r = rand() % 1000000;
            DBGTHRD(<< "Produced value: " << r);
            memcpy(b, &r, 4);
            usleep(4000 + (rand() % 3000)); // between 4-7ms
            buf->producerRelease();
        }
    }
};

class consumer_t : public smthread_t {
    void run() {
        char* b = buf->consumerRequest();
        while (b) {
            uint32_t r;
            memcpy(&r, b, 4);
            DBGTHRD(<< "Consumed value: " << r);
            usleep(4000 + (rand() % 3000)); // between 4-7ms
            buf->consumerRelease();
            b = buf->consumerRequest();
        }
    }
};

int main(int, char **)
{
    consumer_t* c = new consumer_t();
    producer_t* p = new producer_t();

    p->fork();
    c->fork();
    sleep(5);
    buf->set_finished();
    p->join();
    c->join();
}
