#include <assert.h>
#include "avl_tree.h"



static uint32_t max(uint32_t a, uint32_t b){
    return a>b? a: b;
}

static void avl_update(AVLNode *node){
    node->height= max(avl_height(node->left), avl_height(node->right))+1;
    node->cnt= avl_cnt(node->left)+avl_cnt(node->right)+1;
}

static AVLNode *rotate_left(AVLNode *node){
    AVLNode *parent = node->parent;
    AVLNode *new_node= node->right;
    AVLNode *inner = new_node->left;
    node->right= inner;
    if(inner){
        inner->parent=node;
    }
    new_node->parent=parent;
    new_node->left=node;
    node->parent=new_node;
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rotate_right(AVLNode *node){
    AVLNode *parent = node->parent;
    AVLNode *new_node= node->left;
    AVLNode *inner = new_node->right; 
    node->left= inner;
    if(inner){
        inner->parent=node;
    }
    new_node->parent=parent;
    new_node->right=node;
    node->parent=new_node;
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *fix_left(AVLNode *node){
    if(avl_height(node->left->left) >= avl_height(node->left->right)){
        node->left=rotate_left(node->left);
    }
    return rotate_right(node);   
}


static AVLNode *fix_right(AVLNode *node){
    if(avl_height(node->right->right) < avl_height(node->right->left)){
        node->right=rotate_right(node->right);
    }
    return rotate_left(node);   
}

AVLNode *avl_fix(AVLNode *node){
    while(true){
        AVLNode **from= &node;
        AVLNode *parent= node->parent;
        if(parent){
            from= parent->left==node? &parent->left : &parent->right;
        }

        avl_update(node);
        uint32_t l= avl_height(node->left);
        uint32_t r=avl_height(node->right);
        if(l== r+2){
            *from= fix_left(node);
        }
        else if(r== l+2){
            *from= fix_right(node);
        }
        
        if(!parent){
            return *from;
        }

        node=parent;
            
    }     
}

static AVLNode *avl_del_easy(AVLNode *node) {
    assert(!node->left || !node->right);   
    AVLNode *child = node->left ? node->left : node->right; 
    AVLNode *parent = node->parent;

    if (child) {
        child->parent = parent; 
    }
    
    if (!parent) {
        return child;   
    }
    AVLNode **from = parent->left == node ? &parent->left : &parent->right;
    *from = child;

    return avl_fix(parent);
}

AVLNode *avl_delete(AVLNode *node) {

    if (!node->left || !node->right) {
        return avl_del_easy(node);
    }
    
    AVLNode *victim = node->right;
    while (victim->left) {
        victim = victim->left;
    }
    
    AVLNode *root = avl_del_easy(victim);
    
    *victim = *node;    
    if (victim->left) {
        victim->left->parent = victim;
    }
    if (victim->right) {
        victim->right->parent = victim;
    }
    
    AVLNode **from = &root;
    AVLNode *parent = node->parent;
    if (parent) {
        from = parent->left == node ? &parent->left : &parent->right;
    }
    *from = victim;
    return root;
}
