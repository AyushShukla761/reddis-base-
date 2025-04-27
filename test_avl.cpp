#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <set>
#include "avl_tree.h"
#include "avl_tree.cpp"
#include <iostream>

using namespace std;


#define container_of(ptr, type, member) ({\
    const typeof( ((type *)0)->member ) *__mptr = (ptr);\
    ((type *)((char *)(ptr) - offsetof(type, member)));})


struct Data {
    AVLNode node;
    uint32_t val=0;
};

struct container {
    AVLNode *root= nullptr;
};


static void add(container &c, uint32_t val) {
    Data *data = new Data();
    data->val = val;
    avl_init(&data->node);
    
    AVLNode *cur=NULL;
    auto from = &c.root;
    while(*from){
        cur= *from;
        uint32_t cur_val= container_of(cur, Data, node)->val;
        from= (val< cur_val)? &cur->left : &cur->right;
    }
    *from= &data->node;
    data->node.parent= cur;
    c.root= avl_fix(c.root);
}

static bool del(container &c, uint32_t val) {
    AVLNode *cur= c.root;
    while(cur){
        uint32_t cur_val= container_of(cur, Data, node)->val;
        if(val== cur_val){
            break;
        }
        cur= (val< cur_val)? cur->left : cur->right;
    }
    if(!cur){
        return false;
    }
    c.root= avl_delete(cur);
    delete container_of(cur, Data, node);
    return true;
}


static void avl_verify(AVLNode *parent, AVLNode *node) {
    if(!node) {
        return;
    }

    assert(node->parent == parent);
    avl_verify(node, node->left);
    avl_verify(node, node->right);  

    uint32_t l = avl_height(node->left);
    uint32_t r = avl_height(node->right);
    assert(l==r || l==r+1 || l==r-1);
    assert(node->height == 1+std::max(l, r));

    uint32_t val= container_of(node, Data, node)->val;
    if(node->left) {
        assert(node->left->parent == node);
        assert(val >= container_of(node->left, Data, node)->val);
    }
    if(node->right) {
        assert(node->right->parent == node);
        assert(val <= container_of(node->right, Data, node)->val);
    }
}



static void extract (AVLNode *node, std::multiset<uint32_t> &extracted) {
    if(!node) {
        return;
    }
    extract(node->left, extracted);
    extracted.insert(container_of(node, Data, node)->val);
    extract(node->right, extracted);
}

static void container_verify(container &c, const std:: multiset<uint32_t> &ref) {
    avl_verify(nullptr, c.root);
    assert(avl_cnt(c.root) == ref.size());
    std::multiset<uint32_t> extracted;
    extract(c.root, extracted);
    assert(ref == extracted);
}

static void dispose(container &c) {
    AVLNode *cur= c.root;
    while(cur){
        cur= avl_delete(cur);
        delete container_of(cur, Data, node);
    }
}

static void test_insert(uint32_t sz){
    for(uint32_t i=0; i<sz; i++){
        container c;
        std::multiset<uint32_t> ref;
        for(uint32_t j=0; j<sz; j++){
            if(j==i){
                continue;
            }
            add(c, j);
            ref.insert(j);
        }
        container_verify(c, ref);

        add(c, i);
        ref.insert(i);
        container_verify(c, ref);
        dispose(c);
    }
}

static void test_insert_dup(uint32_t sz){
    for(uint32_t i=0; i<sz; i++){
        container c;
        std::multiset<uint32_t> ref;
        for(uint32_t j=0; j<sz; j++){
            add(c, j);
            ref.insert(j);
        }
        container_verify(c, ref);

        add(c, i);
        ref.insert(i);
        container_verify(c, ref);
        dispose(c);
    }
}

static void test_delete(uint32_t sz){
    for(uint32_t i=0; i<sz; i++){
        container c;
        std::multiset<uint32_t> ref;
        for(uint32_t j=0; j<sz; j++){
            add(c, j);
            ref.insert(j);
        }
        container_verify(c, ref);

        assert(del(c, i));
        ref.erase(i);
        container_verify(c, ref);
        dispose(c);
    }
}


int main(){
    container c;
    container_verify(c, {});
    add(c, 1);
    container_verify(c, {1});
    assert(!del(c, 2));
    assert(del(c, 1));
    container_verify(c, {});
    cout<< "test insert2" << endl;
    
    std:: multiset<uint32_t> ref;
    for(uint32_t i=0; i<1000; i+=3){
        add(c, i);
        ref.insert(i);
        container_verify(c, ref);
        cout<< "test insert" << endl;
    }

    for (uint32_t i = 0; i < 100; i++) {
        uint32_t val = (uint32_t)rand() % 1000;
        add(c, val);
        ref.insert(val);
        container_verify(c, ref);
    }

    // for (uint32_t i = 0; i < 200; i++) {
    //     uint32_t val = (uint32_t)rand() % 1000;
    //     auto it = ref.find(val);
    //     if (it == ref.end()) {
    //         assert(!del(c, val));
    //     } else {
    //         assert(del(c, val));
    //         ref.erase(it);
    //     }
    //     container_verify(c, ref);
    // }

    // for (uint32_t i = 0; i < 200; ++i) {
    //     test_insert(i);
    //     test_insert_dup(i);
    //     test_delete(i);
    // }

    dispose(c);
    return 0;
}