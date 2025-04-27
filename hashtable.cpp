#include "hashtable.h"
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>


static void h_init(HashTable *ht, size_t n) {
    assert(n>0 && (n & (n - 1)) == 0);
    ht->size = 0;
    ht->mask = n  - 1;
    ht->table = (Node **)calloc(n,sizeof(Node *));                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
}

static void h_insert(HashTable *ht, Node *node) {
    size_t pos = node->hash & ht->mask;
    Node *next = ht->table[pos];
    node->next = next;
    ht->table[pos] = node;
    ht->size++;
}

static Node **h_lookup(HashTable *ht, Node *key, bool(*eq)(Node*, Node*)) {
    if(!ht->table) {
        return NULL;
    }
    size_t pos = key->hash & ht->mask;
    Node **from = &ht->table[pos];
    for (Node *cur; (cur = *from) != NULL; from = &cur->next) {
        if (cur->hash == key->hash && eq(cur, key)) {
            return from;     
        }
    }
    return NULL;
}

static Node * h_detach(HashTable *ht, Node** from){
    Node *cur = *from;
    *from = cur->next;
    ht->size--;
    return cur;
}

const size_t k_rehashing_work = 128;

static void hm_help_rehash(HashMap *hmap) {
    size_t n = 0;
    while(n < k_rehashing_work && hmap->older.size > 0) {
        Node **from = &hmap->older.table[hmap->migrate_pos];
        if(!*from) {
            hmap->migrate_pos++;
            continue;
        }
        h_insert(&hmap->newer, h_detach(&hmap->older, from));
        n++;
    }
    if(hmap->older.size == 0 && hmap->older.table) {
        free(hmap->older.table);
        hmap->older = HashTable{};
    }
}

static void hm_trigger_rehash(HashMap *hmap) {
    assert(hmap->older.table == NULL);
    hmap->older= hmap->newer;
    h_init(&hmap->newer,( hmap->newer.mask+1) * 2);
    hmap->migrate_pos = 0;
}

Node *hm_lookup(HashMap *hmap, Node *key, bool(*eq)(Node*, Node*)) {
    hm_help_rehash(hmap);
    Node **from = h_lookup(&hmap->newer, key, eq);
    if(!from) {
        from = h_lookup(&hmap->older, key, eq);
    }    
    return from? *from : NULL;
}

const size_t k_max_load = 8;

void hm_insert(HashMap *hmap, Node *node) {
    if(!hmap->newer.table) {
        h_init(&hmap->newer, 4);
    }
    h_insert(&hmap->newer, node);

    if(!hmap->older.table) {
        size_t thresold= k_max_load * (hmap->newer.mask + 1);
        if(hmap->newer.size >= thresold) {
            hm_trigger_rehash(hmap);
        }
    }
    hm_help_rehash(hmap);
}

Node* hm_delete(HashMap *hmap, Node *key, bool(*eq)(Node*, Node*)) {
    hm_help_rehash(hmap);
    Node **from = h_lookup(&hmap->newer, key, eq);
    if(from) {
        return h_detach(&hmap->newer, from);
    }
    from = h_lookup(&hmap->older, key, eq);
    if(from) {
        return h_detach(&hmap->older, from);
    }
    return NULL;
}

void hm_clear(HashMap *hmap) {
    free(hmap->newer.table);
    free(hmap->older.table);
    *hmap = HashMap{};
}

size_t hm_size(HashMap *hmap) {
    return hmap->newer.size + hmap->older.size;
}

static bool h_foreach(HashTable *ht, bool(*cb)(Node*, void*), void *arg) {
    for(size_t i=0;ht->mask!=0 && i<= ht->mask; i++){
        for(Node *cur= ht->table[i]; cur!= NULL; cur= cur->next){
            if(!cb(cur, arg)){
                return false;
            }
        }
    }
    return true;
}

void hm_foreach(HashMap *hmap, bool(*cb)(Node*, void*), void *arg) {
    h_foreach(&hmap->newer, cb, arg) && h_foreach(&hmap->older, cb, arg);
}