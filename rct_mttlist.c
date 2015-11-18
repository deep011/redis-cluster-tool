
#include "rct_core.h"

mttlist *mttlist_create(void)
{
    mttlist *list;

    list = rct_alloc(sizeof(*list));
    if(list == NULL)
    {
        return NULL;
    }

    list->l = NULL;
    list->lock_push = NULL;
    list->lock_pop = NULL;
    list->free = NULL;
    list->length = NULL;
    
    return list;
}

void mttlist_destroy(mttlist *list)
{
    if(list == NULL)
    {
        return;
    }

    if(list->free)
    {
        list->free(list->l);
    }

    rct_free(list);
}

int mttlist_push(mttlist *list, void *value)
{
    if(list == NULL || list->l == NULL
        || list->lock_push == NULL)
    {
        return RCT_ERROR;
    }

    return list->lock_push(list->l, value);
}

void *mttlist_pop(mttlist *list)
{
    if(list == NULL || list->l == NULL
        || list->lock_pop == NULL)
    {
        return NULL;
    }
    
    return list->lock_pop(list->l);
}

int mttlist_empty(mttlist *list)
{
    if(list == NULL || list->l == NULL
        || list->length == NULL)
    {
        return RCT_ERROR;
    }

    if(list->length(list->l) > 0)
    {
        return 0;
    }

    return 1;
}


