#include "bst.h"
// 在二叉查找树中插入一个节点
SSTable* bstInsert(SSTable* root, SSTable* sst) {
    if (root == NULL) {
        return sst;
    }
    if (sst->smallest_key < root->smallest_key) {
        root->next_table = bstInsert(root->next_table, sst);
    } else if (sst->smallest_key > root->smallest_key) {
        root->next_table_right = bstInsert(root->next_table_right, sst);
    }
    return root;
}

// 在二叉查找树中查找刚好大于上一次compaction节点的节点
SSTable* bstSearchHigherTable(SSTable* root, KeyType last_smallest_key)
{
    if(root == NULL)
        return NULL;
    if(root->smallest_key < last_smallest_key)
    {
        return bstSearchHigherTable(root->next_table_right, last_smallest_key);
    }
    else 
    {
        if(root->next_table == NULL)
        {
            return root;
        }
        else
        {
            SSTable* temp = bstSearchHigherTable(root->next_table, last_smallest_key);
            if(temp == NULL)
                return root;
            else
                return temp;
        }
    }
}


// 在二叉查找树中查找一个节点
SSTable* bstSearch(SSTable* root, KeyType key) {
    if (root == NULL || (key >= root->smallest_key && key <= root->largest_key)) {
        return root;
    }
    if (key < root->smallest_key) {
        return bstSearch(root->next_table, key);
    } 
    else if (key > root->largest_key)
    {
        return bstSearch(root->next_table_right, key);
    }
}

void bstRangeSearch(SSTable* root, KeyType low, KeyType high, uint64_t* input_ids_i1, uint64_t* input_nums_i1, int* level_i1_index)
{
    // printf("%d\n", *level_i1_index);
    if(root == NULL)
        return;
    if(low < root->smallest_key)
        bstRangeSearch(root->next_table, low, high, input_ids_i1, input_nums_i1, level_i1_index);
    if (low <= root->largest_key && high >= root->smallest_key && !root->is_selected)
    {
        struct stat64 st;
        int ret = stat64(root->table_name, &st);
        root->is_selected = true;
        assert(ret == 0);
        input_ids_i1[*level_i1_index] = st.st_ino;
        input_nums_i1[*level_i1_index] = root->table_num;
        (*level_i1_index)++;
    }
    if(high > root->largest_key)
        bstRangeSearch(root->next_table_right, low, high, input_ids_i1, input_nums_i1, level_i1_index);
}

SSTable* findBstMin(SSTable* node) {
    if(node == NULL)
        return NULL;
    while (node->next_table != NULL) {
        node = node->next_table;
    }
    return node;
}

// 中序遍历二叉查找树并删除节点
SSTable* deleteBstNode(SSTable* root, uint64_t table_num, SSTable** deleted, bool& fl) {
    if (root != NULL) {
        if(root->table_num == table_num)
        {
            fl = 1;
            if(deleted != NULL)
                *deleted = root;
            if (root->next_table == NULL) {
                SSTable* temp = root->next_table_right;
                // free(root);
                return temp;
            } else if (root->next_table_right == NULL) {
                SSTable* temp = root->next_table;
                // free(root);
                return temp;
            }
            SSTable* temp = findBstMin(root->next_table_right);
            bool flag = 0;
            root->next_table_right = deleteBstNode(root->next_table_right, temp->table_num, NULL, flag);
            temp->next_table = root->next_table;
            temp->next_table_right = root->next_table_right;
            root = temp;
        }
        else
        {
            if(!fl)
                root->next_table = deleteBstNode(root->next_table, table_num, deleted, fl);
            if(!fl)
                root->next_table_right = deleteBstNode(root->next_table_right, table_num, deleted, fl);
        }
        
    }
    return root;
}

// 中序遍历二叉查找树
void inorderTraversal(SSTable* root) {
    if (root != NULL) {
        inorderTraversal(root->next_table);
        printf("%ld, %ld, %ld, %s\n", root->table_num, root->smallest_key, root->largest_key, root->table_name);
        inorderTraversal(root->next_table_right);
    }
}

// int main()
// {
//     SSTable* root = NULL;
//     SSTable sst;
//     // 插入一些节点
//     sst.fd = 1; sst.smallest_key = 50; sst.largest_key = 60;
//     root = bstInsert(root, &sst);
//     sst.fd = 2; sst.smallest_key = 10; sst.largest_key = 20;
//     root = bstInsert(root, &sst);
//     sst.fd = 3; sst.smallest_key = 70; sst.largest_key = 80;
//     root = bstInsert(root, &sst);
//     sst.fd = 4; sst.smallest_key = 30; sst.largest_key = 40;
//     root = bstInsert(root, &sst);
//     sst.fd = 5; sst.smallest_key = 5; sst.largest_key = 9;
//     root = bstInsert(root, &sst);
//     sst.fd = 6; sst.smallest_key = 90; sst.largest_key = 100;
//     root = bstInsert(root, &sst);

//     SSTable* s = bstSearchHigherTable(root, 4);
//     printf("%d, %ld\n", s->fd, s->smallest_key);
//     // for(int i = 0; i < 10; i++)
//     // {
//     //     sst.fd++;
//     //     sst.smallest_key = sst.largest_key + 1;
//     //     sst.largest_key = sst.smallest_key + 5;
//     //     root = bstInsert(root, &sst);
//     // }

//     // SSTable* sstt = bstSearch(root, 10);
//     // printf("%d, %ld\n", sstt->fd, sstt->smallest_key);
//     // root = deleteBstNode(root, 1);
//     // root = deleteBstNode(root, 3);
//     // root = deleteBstNode(root, 4);
//     // inorderTraversal(root);
//     // int inputs_id_i1[20];
//     // int level_index_i1 = 0;
//     // bstRangeSearch(root, 10, 40, inputs_id_i1, &level_index_i1);
//     // printf("%d\n", level_index_i1);
//     // for(int i = 0; i < level_index_i1; i++)
//     // {
//     //     printf("%d ", inputs_id_i1[i]);
//     // }
//     // printf("\n");
//     return 0;
// }
