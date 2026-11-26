#define _POSIX_C_SOURCE 200809L

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define EVENTS_COUNT 5

#define CHECK(call)                                                      \
    do {                                                                 \
        int rc_ = (call);                                                \
        if (rc_ != 0) {                                                  \
            fprintf(stderr, "%s failed: %s\n", #call, strerror(rc_));    \
            exit(EXIT_FAILURE);                                          \
        }                                                                \
    } while (0)

typedef struct {
    int id;
    char text[64];
} Event;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t event_ready;
    pthread_cond_t slot_free;
    int ready;
    int stopping;
    Event *data;
} Monitor;

static Monitor monitor = {
    PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
    0,
    0,
    NULL
};

static void monitor_provide(Monitor *m, Event *ev)
{
    CHECK(pthread_mutex_lock(&m->mutex));

    while (m->ready)
        CHECK(pthread_cond_wait(&m->slot_free, &m->mutex));

    m->data = ev;
    m->ready = 1;

    printf("[поставщик]  событие #%d отправлено\n", ev->id);
    fflush(stdout);

    CHECK(pthread_cond_signal(&m->event_ready));
    CHECK(pthread_mutex_unlock(&m->mutex));
}

static Event *monitor_consume(Monitor *m)
{
    Event *ev;

    CHECK(pthread_mutex_lock(&m->mutex));

    while (!m->ready && !m->stopping)
        CHECK(pthread_cond_wait(&m->event_ready, &m->mutex));

    if (!m->ready) {
        CHECK(pthread_mutex_unlock(&m->mutex));
        return NULL;
    }

    ev = m->data;
    m->data = NULL;
    m->ready = 0;

    printf("[потребитель] событие #%d получено: %s\n", ev->id, ev->text);
    fflush(stdout);

    CHECK(pthread_cond_signal(&m->slot_free));
    CHECK(pthread_mutex_unlock(&m->mutex));

    return ev;
}

static void monitor_stop(Monitor *m)
{
    CHECK(pthread_mutex_lock(&m->mutex));
    m->stopping = 1;
    CHECK(pthread_cond_broadcast(&m->event_ready));
    CHECK(pthread_cond_broadcast(&m->slot_free));
    CHECK(pthread_mutex_unlock(&m->mutex));
}

static void *producer_run(void *arg)
{
    Monitor *m = arg;

    for (int i = 1; i <= EVENTS_COUNT; i++) {
        sleep(1);

        Event *ev = malloc(sizeof(*ev));
        if (ev == NULL) {
            fprintf(stderr, "malloc failed\n");
            exit(EXIT_FAILURE);
        }
        ev->id = i;
        snprintf(ev->text, sizeof(ev->text), "данные события %d", i);

        monitor_provide(m, ev);
    }

    monitor_stop(m);
    return NULL;
}

static void *consumer_run(void *arg)
{
    Monitor *m = arg;
    Event *ev;

    while ((ev = monitor_consume(m)) != NULL) {
        free(ev);
    }
    return NULL;
}

int main(void)
{
    pthread_t producer, consumer;

    CHECK(pthread_create(&consumer, NULL, consumer_run, &monitor));
    CHECK(pthread_create(&producer, NULL, producer_run, &monitor));

    CHECK(pthread_join(producer, NULL));
    CHECK(pthread_join(consumer, NULL));

    CHECK(pthread_cond_destroy(&monitor.event_ready));
    CHECK(pthread_cond_destroy(&monitor.slot_free));
    CHECK(pthread_mutex_destroy(&monitor.mutex));

    return 0;
}
