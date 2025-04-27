#pragma once

#include <stddef.h>
#include <stdint.h> 

using namespace std;

struct Node{
    Node * next= nullptr;
    uint64_t hash= 0;
};

struct  HashTable{
    Node ** table= nullptr;
    size_t size= 0;
    size_t mask= 0;
};


struct HashMap{
    HashTable newer;
    HashTable older;
    size_t migrate_pos= 0;
};

Node *hm_lookup(HashMap *hmap, Node *key, bool(*eq)(Node*, Node*));
void hm_insert(HashMap *hmap, Node *node);
Node* hm_delete(HashMap *hmap, Node *key, bool(*eq)(Node*, Node*)); 
void hm_clear(HashMap *hmap);
size_t hm_size(HashMap *hmap);

void hm_foreach(HashMap *hmap, bool(*cb)(Node*, void*), void *arg);