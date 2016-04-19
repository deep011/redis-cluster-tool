#ifndef _RCT_LOCKLIST_H_
#define _RCT_LOCKLIST_H_

typedef struct locklist{
    hilist *l;
    pthread_mutex_t lmutex;
}locklist;

locklist *locklist_create(void);
int locklist_push(void *l, void *value);
void *locklist_pop(void *l);
void locklist_free(void *l);
long long locklist_length(void *l);

int mttlist_init_with_locklist(mttlist *list);

#endif
