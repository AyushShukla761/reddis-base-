#pragma once

#include <stddef.h>
#include <stdint.h>


struct AVLNode {
    AVLNode *left = nullptr;
    AVLNode *right = nullptr;
    AVLNode *parent = nullptr;
    uint32_t cnt = 0;
    uint32_t height = 0;
};


inline void avl_init(AVLNode *node){
    node->left=node->right=node->parent=nullptr;
    node->height=0;
    node->cnt=1;
}

inline uint32_t avl_height(AVLNode *node){
    return node? node->height : 0;
}
inline uint32_t avl_cnt(AVLNode *node){
    return node? node->cnt : 0;
}  

AVLNode *avl_fix(AVLNode *node);
AVLNode *avl_delete(AVLNode *node);
