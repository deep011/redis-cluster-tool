#ifndef _RCT_MTTLIST_H_
#define _RCT_MTTLIST_H_

typedef struct mttlist{
    void *l;
    int (*lock_push)(void *l, void *value);
    void *(*lock_pop)(void *l);
    void (*free)(void *l);
    long long (*length)(void *l);
}mttlist;

mttlist *mttlist_create(void);
void mttlist_destroy(mttlist *list);
int mttlist_push(mttlist *list, void *value);
void *mttlist_pop(mttlist *list);

int mttlist_empty(mttlist *list);

#endif
