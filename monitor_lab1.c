#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define EVENTS_COUNT 5

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

Monitor monitor = {
    PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
    PTHREAD_COND_INITIALIZER,
    0,
    0,
    NULL
};

void monitor_provide(Monitor *m, Event *ev)
{
    pthread_mutex_lock(&m->mutex);

    while (m->ready) {
        pthread_cond_wait(&m->slot_free, &m->mutex);
    }

    m->data = ev;
    m->ready = 1;

    printf("[поставщик]  событие #%d отправлено\n", ev->id);

    pthread_cond_signal(&m->event_ready);
    pthread_mutex_unlock(&m->mutex);
}

Event *monitor_consume(Monitor *m)
{
    Event *ev;

    pthread_mutex_lock(&m->mutex);

    while (!m->ready && !m->stopping) {
        pthread_cond_wait(&m->event_ready, &m->mutex);
    }

    if (!m->ready) {
        pthread_mutex_unlock(&m->mutex);
        return NULL;
    }

    ev = m->data;
    m->data = NULL;
    m->ready = 0;

    printf("[потребитель] событие #%d получено: %s\n", ev->id, ev->text);

    pthread_cond_signal(&m->slot_free);
    pthread_mutex_unlock(&m->mutex);

    return ev;
}

void monitor_stop(Monitor *m)
{
    pthread_mutex_lock(&m->mutex);
    m->stopping = 1;
    pthread_cond_broadcast(&m->event_ready);
    pthread_cond_broadcast(&m->slot_free);
    pthread_mutex_unlock(&m->mutex);
}

void *producer_run(void *arg)
{
    Monitor *m = arg;
    int i;

    for (i = 1; i <= EVENTS_COUNT; i++) {
        if (i > 1) {
            sleep(1);
        }

        Event *ev = malloc(sizeof(Event));
        if (ev == NULL) {
            printf("Ошибка выделения памяти\n");
            exit(1);
        }
        ev->id = i;
        snprintf(ev->text, sizeof(ev->text), "данные события %d", i);

        monitor_provide(m, ev);
    }

    monitor_stop(m);
    return NULL;
}

void *consumer_run(void *arg)
{
    Monitor *m = arg;

    Event *ev = monitor_consume(m);
    while (ev != NULL) {
        free(ev);
        ev = monitor_consume(m);
    }
    return NULL;
}

int main(void)
{
    pthread_t producer, consumer;

    pthread_create(&consumer, NULL, consumer_run, &monitor);
    pthread_create(&producer, NULL, producer_run, &monitor);

    pthread_join(producer, NULL);
    pthread_join(consumer, NULL);

    pthread_cond_destroy(&monitor.event_ready);
    pthread_cond_destroy(&monitor.slot_free);
    pthread_mutex_destroy(&monitor.mutex);

    return 0;
}
