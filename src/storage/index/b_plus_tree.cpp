#include <memory>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"
#include "type/value.h"

#define INSERT_FUNCTION 0
#define DELETE_FUNCTION 1
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator) {
  leaf_max_size_ = leaf_max_size < 250 ? leaf_max_size : 250;
  internal_max_size_ = internal_max_size < 250 ? internal_max_size : 250;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetValue1(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool
// {
//   if (IsEmpty()) {
//     return false;
//   }
//   auto target_leaf_pageid = FindLeafPage(root_page_id_, key);
//   auto target_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(target_leaf_pageid));
//   bool res = target_leaf_page->Find(key, result, comparator_);
//   buffer_pool_manager_->UnpinPage(target_leaf_pageid, false);
//   return res;
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> LeafPage * {
  printf("findLeafPage\n");
  BPlusTreePage *target_page = nullptr;
  while (target_page == nullptr || !target_page->IsRootPage()) {
    if (target_page != nullptr) {
      target_page->GetLatch().RUnlock();
      buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
    }
    share_root_latch_.lock_shared();
    target_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    share_root_latch_.unlock_shared();
    target_page->GetLatch().RLock();
  }
  while (!target_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(target_page);
    for (int index = 1; index < internal_page->GetSize(); index++) {
      int compare_result = comparator_(key, internal_page->KeyAt(index));
      if ((compare_result < 0 && index == 1) || (compare_result >= 0 && index == internal_page->GetSize() - 1) ||
          (compare_result >= 0 && comparator_(key, internal_page->KeyAt(index + 1)) < 0)) {
        int res_index = (index == 1 && compare_result < 0) ? index - 1 : index;

        target_page =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(res_index)));
        target_page->GetLatch().RLock();
        internal_page->GetLatch().RUnlock();
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
        break;
      }
    }
  }

  return reinterpret_cast<LeafPage *>(target_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::WriteFindLeafPage(const KeyType &key, Transaction *transaction, int call_fun) -> BPlusTreePage * {
  bool pessimistic_flag;
  // printf("OPTwritepage\n");
  BPlusTreePage *target_page = nullptr;
  while (target_page == nullptr || target_page->GetPageId() != root_page_id_) {
    if (target_page != nullptr) {
      if (target_page->IsLeafPage()) {
        target_page->GetLatch().WUnlock();
      } else {
        target_page->GetLatch().RUnlock();
      }
      buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
    }
    share_root_latch_.lock_shared();
    pessimistic_flag = false;
    target_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    share_root_latch_.unlock_shared();
    if (target_page->IsLeafPage()) {
      target_page->GetLatch().WLock();
      if ((target_page->GetSize() == target_page->GetMaxSize() && call_fun == INSERT_FUNCTION) ||
          (target_page->GetSize() <= target_page->GetMinSize() && call_fun == DELETE_FUNCTION)) {
        pessimistic_flag = true;
      }
    } else {
      target_page->GetLatch().RLock();
    }
  }
  // printf("current node's children num:%d \n", target_page->GetSize());
  while (!target_page->IsLeafPage() && !pessimistic_flag) {
    auto internal_page = reinterpret_cast<InternalPage *>(target_page);
    for (int index = 1; index < internal_page->GetSize(); index++) {
      int compare_result = comparator_(key, internal_page->KeyAt(index));

      // printf("%d ", internal_page->ValueAt(index));
      if ((compare_result < 0 && index == 1) || (compare_result >= 0 && index == internal_page->GetSize() - 1) ||
          (compare_result >= 0 && comparator_(key, internal_page->KeyAt(index + 1)) < 0)) {
        int res_index = (index == 1 && compare_result < 0) ? index - 1 : index;
        target_page =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(res_index)));
        if (target_page->IsLeafPage()) {
          target_page->GetLatch().WLock();
        } else {
          target_page->GetLatch().RLock();
        }
        internal_page->GetLatch().RUnlock();
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
        // printf("(找到目标节点:%d)", target_page->GetPageId());
        // if (internal_page->IsRootPage()) {
        //   share_root_latch_.unlock_shared();
        // }
        if ((target_page->GetSize() == target_page->GetMaxSize() && call_fun == INSERT_FUNCTION) ||
            (target_page->GetSize() <= target_page->GetMinSize() && call_fun == DELETE_FUNCTION)) {
          pessimistic_flag = true;
        }
        break;
      }
    }
    // printf("\n");
  }
  //  悲观写
  if (pessimistic_flag) {
    // printf("(key %ld)need PESS write\n", key.ToString());
    if (target_page->IsLeafPage()) {
      target_page->GetLatch().WUnlock();
    } else {
      target_page->GetLatch().RUnlock();
    }
    buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
    return nullptr;
  }
  return target_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  // printf("Getvalue\n");
  auto target_leaf_page = FindLeafPage(key);
  bool res = target_leaf_page->Find(key, result, comparator_);
  target_leaf_page->GetLatch().RUnlock();
  buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetNeedUpdatePage(const KeyType &key, Transaction *transaction, int call_fun) {
  BPlusTreePage *target_page = nullptr;

  while (target_page == nullptr || target_page->GetPageId() != root_page_id_) {
    if (target_page != nullptr) {
      target_page->GetLatch().WUnlock();
      buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
    }
    share_root_latch_.lock();
    target_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    share_root_latch_.unlock();
    target_page->GetLatch().WLock();
  }
  // printf("GetNeedPageFun():");
  // target_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));

  // printf("wlock page:%d ", target_page->GetPageId());
  transaction->AddIntoPageSet(reinterpret_cast<Page *>(target_page));
  while (!target_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(target_page);
    // printf("当前节点的孩子数量:%d -->\n",internal_page->GetSize());
    for (int index = 1; index < internal_page->GetSize(); index++) {
      // printf("%d ", internal_page->ValueAt(index));
      int compare_result = comparator_(key, internal_page->KeyAt(index));
      if ((compare_result < 0 && index == 1) || (compare_result >= 0 && index == internal_page->GetSize() - 1) ||
          (compare_result >= 0 && comparator_(key, internal_page->KeyAt(index + 1)) < 0)) {
        int res_index = (index == 1 && compare_result < 0) ? index - 1 : index;
        target_page =
            reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(res_index)));
        // assert(target_page != nullptr);
        target_page->GetLatch().WLock();  //  kazhele

        // printf(" success ");
        // printf("%d ", target_page->GetPageId());
        if ((target_page->GetSize() < target_page->GetMaxSize() && call_fun == INSERT_FUNCTION) ||
            (target_page->GetSize() > target_page->GetMinSize() && call_fun == DELETE_FUNCTION)) {  // safe，解父节点锁
          auto page_set = transaction->GetPageSet();
          // printf("unlock parent:(");
          while (!page_set->empty()) {
            // printf("\ncan unlock parent:");
            auto parent_page = reinterpret_cast<BPlusTreePage *>(transaction->GetPageSet()->back());
            parent_page->GetLatch().WUnlock();
            // if (parent_page->IsRootPage()) {
            //   share_root_latch_.unlock();
            // }
            // printf(" %d ", parent_page->GetPageId());
            transaction->GetPageSet()->pop_back();
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
          }
          // printf(")");
        }
        transaction->AddIntoPageSet(reinterpret_cast<Page *>(target_page));

        break;
      }
    }
    // printf("\n");
  }
  // printf("lock finish.\n");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInternalArrayHelper(std::pair<KeyType, page_id_t> *array, int array_size, const KeyType &key,
                                               page_id_t value) {
  int index = 1;
  while (index < array_size) {
    if (comparator_(key, array[index].first) < 0) {
      break;
    }
    index++;
  }
  for (int move = array_size - 1; move >= index; move--) {
    array[move + 1] = array[move];
  }
  array[index].first = key;
  array[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertLeafArrayHelper(MappingType *array, int array_size, const KeyType &key,
                                           const ValueType &value) {
  int index = 0;
  while (index < array_size) {
    if (comparator_(key, array[index].first) < 0) {
      break;
    }
    index++;
  }
  for (int move = array_size - 1; move >= index; move--) {
    array[move + 1] = array[move];
  }
  array[index].first = key;
  array[index].second = value;
}

// 设置child的父亲节点，如果在数组左半边的说明父亲为左边节点，否则父亲为右边新创建的节点
// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::SetNewParentPageId(std::pair<KeyType, page_id_t> *array, int array_size, BPlusTreePage *child,
//                                         InternalPage *left_parent, InternalPage *right_parent) {
//   for (int index = 0; index <= array_size; index++) {
//     if (child->GetPageId() == array[index].second) {
//       child->SetParentPageId(left_parent->GetPageId());
//       return;
//     }
//   }
//   child->SetParentPageId(right_parent->GetPageId());
// }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left, const KeyType &key, BPlusTreePage *right,
                                    Transaction *transaction) {
  // printf("insertInParent, left pageid: %d, right pageid: %d.", left->GetPageId(), right->GetPageId());
  // 一种情况是叶子节点为根节点已满，需要分裂叶节点，同时产生一个新的内部节点作为根节点
  InternalPage *new_internal_page;
  if (left->IsRootPage()) {
    share_root_latch_.lock();
    new_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_));
    left->SetParentPageId(root_page_id_);
    right->SetParentPageId(root_page_id_);
    new_internal_page->Init(root_page_id_, -1, internal_max_size_);
    new_internal_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_internal_page->SetKeyAt(1, key);
    new_internal_page->SetValueAt(0, left->GetPageId());
    new_internal_page->SetValueAt(1, right->GetPageId());
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    share_root_latch_.unlock();
    // printf("generate new root.old root id: %d , new root id: %d.\n", left->GetPageId());
    // new_internal_page->GetPageId());
  } else {  // 第二种是父节点是内部节点，直接插入；
    // printf("insert in internal,left page id %d insert in parent page %d.\n", left->GetPageId(),
    //  left->GetParentPageId());
    page_id_t new_internal_pageid;
    auto parent_internal_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left->GetParentPageId()));
    if (!parent_internal_page->InsertInInternal(key, right->GetPageId(),
                                                comparator_)) {  // 同时若节点已满需要分裂内部节点同时插入父节点的父节点
      std::pair<KeyType, page_id_t> array[internal_max_size_ + 1];
      parent_internal_page->CopyToArray(array);
      array[0].first.SetFromInteger(0);
      InsertInternalArrayHelper(array, internal_max_size_, key, right->GetPageId());
      new_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_internal_pageid));
      new_internal_page->Init(new_internal_pageid, -1, internal_max_size_);

      new_internal_page->SetPageType(IndexPageType::INTERNAL_PAGE);
      KeyType mid_internal_key = array[internal_max_size_ / 2 + 1].first;
      array[internal_max_size_ / 2 + 1].first.SetFromInteger(0);  // 数组中这个位置的中间值会放到父节点中
      parent_internal_page->InsertArray(array, 0, internal_max_size_ / 2);
      new_internal_page->InsertArray(array, internal_max_size_ / 2 + 1, internal_max_size_);

      SetArrayNewParentPageId(array, 0, internal_max_size_ / 2, parent_internal_page);
      SetArrayNewParentPageId(array, internal_max_size_ / 2 + 1, internal_max_size_, new_internal_page);
      InsertInParent(reinterpret_cast<BPlusTreePage *>(parent_internal_page), mid_internal_key,
                     reinterpret_cast<BPlusTreePage *>(new_internal_page), transaction);
      buffer_pool_manager_->UnpinPage(new_internal_pageid, true);
    } else {
      right->SetParentPageId(parent_internal_page->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    // printf("insert in parent success, now quit.left: %d \n", left->GetPageId());
  }
}

/**
 * 每次生成新节点时，需要初始化，包括页id，父节点页id，节点类型更新，
 * 若为新的根节点还需要更新root_page_id_
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertHelper(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto page_set = transaction->GetPageSet();
  auto target_leaf_page = reinterpret_cast<LeafPage *>(WriteFindLeafPage(key, transaction, INSERT_FUNCTION));
  bool pess_write = false;
  if (target_leaf_page == nullptr) {  //  说明需要悲观写
    GetNeedUpdatePage(key, transaction, INSERT_FUNCTION);
    target_leaf_page = reinterpret_cast<LeafPage *>(page_set->back());
    pess_write = true;
  }

  bool result = false;
  // printf("leaf page find success. page_id: %d.\n", target_leaf_page->GetPageId());
  if (target_leaf_page->Find(key, comparator_)) {  //  已有重复键
    // printf("duplicated key.\n");
  } else if (target_leaf_page->GetSize() < leaf_max_size_) {  //  直接插入
    target_leaf_page->InsertInLeaf(key, value, comparator_);
    result = true;
  } else if (target_leaf_page->GetSize() == leaf_max_size_) {  //  如果叶子满了，需要进行分裂
    // printf("left page id %d occurs new split. ", target_leaf_page->GetPageId());
    page_id_t another_leaf_pageid;
    auto another_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&another_leaf_pageid));
    another_leaf_page->Init(another_leaf_pageid, -1, leaf_max_size_);
    MappingType array[leaf_max_size_ + 1];
    target_leaf_page->CopyToArray(array);
    InsertLeafArrayHelper(array, leaf_max_size_, key, value);
    KeyType min_key_in_newleaf = array[leaf_max_size_ / 2 + 1].first;
    target_leaf_page->InsertArray(array, 0, leaf_max_size_ / 2);
    another_leaf_page->SetNextPageId(target_leaf_page->GetNextPageId());
    target_leaf_page->SetNextPageId(another_leaf_pageid);
    another_leaf_page->InsertArray(array, leaf_max_size_ / 2 + 1, leaf_max_size_);
    InsertInParent(reinterpret_cast<BPlusTreePage *>(target_leaf_page), min_key_in_newleaf,
                   reinterpret_cast<BPlusTreePage *>(another_leaf_page), transaction);
    buffer_pool_manager_->UnpinPage(another_leaf_pageid, true);
    result = true;
  }
  if (!pess_write) {
    //  需要手动unpin，因为不再pageset里
    target_leaf_page->GetLatch().WUnlock();
    buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
  }
  // buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
  // printf("insert key %ld success,now quit.\n", key.ToString());
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  share_root_latch_.lock();
  if (IsEmpty()) {
    auto new_root = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_));
    UpdateRootPageId(1);
    new_root->Init(root_page_id_, -1, leaf_max_size_);
    new_root->SetPageType(IndexPageType::LEAF_PAGE);
    new_root->InsertInLeaf(key, value, comparator_);
    // printf("empty root.\n");
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    share_root_latch_.unlock();
    return true;
  }
  share_root_latch_.unlock();
  bool result = InsertHelper(key, value, transaction);
  auto page_set = transaction->GetPageSet();
  // printf("now clear set(has %lu size):", page_set->size());

  while (!page_set->empty()) {
    auto page = reinterpret_cast<BPlusTreePage *>(page_set->back());
    page_set->pop_back();
    page->GetLatch().WUnlock();

    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }

  // printf("\n");
  // printf("   Insert key success,now quit.\n");
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetArrayNewParentPageId(std::pair<KeyType, page_id_t> *array, int low, int high,
                                             InternalPage *parent) {
  // printf("setParent\n");
  // std::cout<<"setParent"<<std::endl;
  int index;
  for (index = low; index <= high; index++) {
    auto childpage = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(array[index].second));
    // assert(childpage != nullptr);
    // printf("%d(%d)  ", childpage->GetPageId(),array[index].second);
    childpage->SetParentPageId(parent->GetPageId());
    buffer_pool_manager_->UnpinPage(array[index].second, true);
  }
  // printf("\n");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
/**第一种情况，兄弟节点>minsize，借走即可；左向右借时，直接用getNextPageid来拿兄弟的key，拿到后在兄弟那边直接修改它自己父节点中的key
 *
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  share_root_latch_.lock();
  if (IsEmpty()) {
    share_root_latch_.unlock();
    return;
  }
  share_root_latch_.unlock();
  // std::cout << "delete: rootid" << root_page_id_ << " key:" << key << std::endl;
  // printf("delete: rootid:%d key:%ld", root_page_id_, key.ToString());
  auto target_leaf_page = WriteFindLeafPage(key, transaction, DELETE_FUNCTION);
  auto page_set = transaction->GetPageSet();
  bool pess_write = false;
  if (target_leaf_page == nullptr) {
    pess_write = true;
    GetNeedUpdatePage(key, transaction, DELETE_FUNCTION);
    target_leaf_page = reinterpret_cast<BPlusTreePage *>(page_set->back());
  }
  // buffer_pool_manager_->FetchPage(target_leaf_page->GetPageId());  //
  // 需要多pin一次，因为进入deleteEntry函数会unpin一次
  DeleteEntry(target_leaf_page, key, nullptr, transaction);
  auto delete_page_set = transaction->GetDeletedPageSet();
  // printf("REMOVE: now remove page from pageset:");
  while (!page_set->empty()) {
    auto page = reinterpret_cast<BPlusTreePage *>(page_set->back());
    page_set->pop_back();
    page->GetLatch().WUnlock();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    if (delete_page_set->find(page->GetPageId()) != delete_page_set->end()) {
      buffer_pool_manager_->DeletePage(page->GetPageId());
      delete_page_set->erase(page->GetPageId());
    }
  }
  if (!pess_write) {  //  乐观写不会引起deletepage或者合并
    target_leaf_page->GetLatch().WUnlock();
    buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
  }
  // printf("remove key %ld finish.\n", key.ToString());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *current_page, const KeyType &key, BPlusTreePage *value,
                                 Transaction *transaction) {
  if (current_page->IsLeafPage()) {
    reinterpret_cast<LeafPage *>(current_page)->Delete(key, comparator_);
  } else if (value != nullptr) {
    reinterpret_cast<InternalPage *>(current_page)->Delete(value->GetPageId());
  }
  if (current_page->IsRootPage() && current_page->GetSize() == 1 &&
      !current_page->IsLeafPage()) {  // N为根节点且删除后只剩一个孩子节点(前提为非叶节点)
    // printf("the root page just has one child,(delete key %ld) now update root,old root is %d ", key.ToString(),
    //  current_page->GetPageId());
    auto target_internal_page = reinterpret_cast<InternalPage *>(current_page);
    // target_internal_page->Delete(value->GetPageId());
    share_root_latch_.lock();
    root_page_id_ =
        target_internal_page->ValueAt(0);  // 每次删除都是删除K和K的右侧指针,所以留下的总是左节点，即0号value
    auto new_root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    // printf("new root is %d\n", new_root_page->GetPageId());
    new_root_page->SetParentPageId(-1);
    UpdateRootPageId();
    // buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);  // 这里多fetch一次了那就需要多unpin一次
    share_root_latch_.unlock();
    transaction->AddIntoDeletedPageSet(current_page->GetPageId());
    // buffer_pool_manager_->DeletePage(current_page->GetPageId());
    // transaction->GetPageSet()->pop_front();  //  根节点肯定在最前
  } else if ((!current_page->IsRootPage() && current_page->IsLeafPage() &&
              current_page->GetSize() < current_page->GetMinSize()) ||
             (!current_page->IsRootPage() && !current_page->IsLeafPage() &&
              current_page->GetSize() < current_page->GetMinSize())) {
    // printf("(key %ld) less than leafminsize.", key.ToString());
    // 或者节点中键值对少于minsize
    auto parent_internal_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(current_page->GetParentPageId()));
    std::pair<KeyType, page_id_t> sibling[2];
    sibling[0].second = -1;
    sibling[1].second = -1;
    parent_internal_page->GetSibling(sibling, current_page->GetPageId());
    page_id_t checked_sibling_index = sibling[0].second == -1 ? 1 : 0;
    auto checked_sibling_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(sibling[checked_sibling_index].second));
    //  把兄弟节点也加入page_set
    checked_sibling_page->GetLatch().WLock();
    transaction->AddIntoPageSet(reinterpret_cast<Page *>(checked_sibling_page));
    // 如果两个节点加起来的pair数量小于maxsize,需要合并， *****注意删除其中一页
    if (checked_sibling_page->GetSize() + current_page->GetSize() <= checked_sibling_page->GetMaxSize()) {
      // printf("(key %ld)need merge", key.ToString());
      if (checked_sibling_index == 1) {  // 节点(N)的兄弟(N')为右兄弟 （也就是N在N'前）,两指针交换
        // 这样做的目的是保持每次都只删除右指针,父节点删除指向右指针的键值对
        auto temp = checked_sibling_page;
        checked_sibling_page = current_page;
        current_page = temp;
      }
      if (!current_page->IsLeafPage()) {  // 非叶子节点，则把父节点的key和N中的所有指针拿到N'中
        auto sibling_internal_page = reinterpret_cast<InternalPage *>(checked_sibling_page);
        auto current_internal_page = reinterpret_cast<InternalPage *>(current_page);
        std::pair<KeyType, page_id_t> array[current_internal_page->GetSize()];
        current_internal_page->CopyToArray(array);
        SetArrayNewParentPageId(array, 0, current_internal_page->GetSize() - 1, sibling_internal_page);
        sibling_internal_page->MergeInInternal(sibling[checked_sibling_index].first, array,
                                               current_internal_page->GetSize());
      } else {  //  叶子节点，则把N中所有K,V拿到N'中,还需注意把nextpageId改下
        auto sibling_leaf_page = reinterpret_cast<LeafPage *>(checked_sibling_page);
        auto current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
        MappingType array[current_leaf_page->GetSize()];
        current_leaf_page->CopyToArray(array);
        sibling_leaf_page->MergeInLeaf(array, current_leaf_page->GetSize());
        sibling_leaf_page->SetNextPageId(current_leaf_page->GetNextPageId());
      }
      DeleteEntry(parent_internal_page, sibling[checked_sibling_index].first, current_page, transaction);
      transaction->AddIntoDeletedPageSet(current_page->GetPageId());
      // buffer_pool_manager_->UnpinPage(current_page->GetPageId(),
      // false);  // false是因为反正这一页是需要删除的，无需浪费时间更新
      // buffer_pool_manager_->DeletePage(current_page->GetPageId());
    } else {  //  兄弟的pair数量充足，从兄弟借一个pair即可，无需分裂
      // std::cout << "兄弟节点充足,向兄弟借kv即可" << std::endl;
      if (checked_sibling_index == 0) {  //  兄弟N‘为节点N的前一个节点 (向左兄弟借pair)，右借左
        if (!current_page->IsLeafPage()) {
          std::pair<KeyType, page_id_t> last_pair_in_sibling;
          auto sibling_internal_page = reinterpret_cast<InternalPage *>(checked_sibling_page);
          auto current_internal_page = reinterpret_cast<InternalPage *>(current_page);
          last_pair_in_sibling.first = sibling_internal_page->KeyAt(sibling_internal_page->GetSize() - 1);
          last_pair_in_sibling.second = sibling_internal_page->ValueAt(sibling_internal_page->GetSize() - 1);
          sibling_internal_page->Delete(last_pair_in_sibling.second);
          parent_internal_page->SwapKeyAtValue(last_pair_in_sibling.first, current_page->GetPageId());
          current_internal_page->InsertInFirst(last_pair_in_sibling.second, last_pair_in_sibling.first);
          // printf("last_pair's value:%d\n",last_pair_in_sibling.second);
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(last_pair_in_sibling.second))
              ->SetParentPageId(current_internal_page->GetPageId());
          buffer_pool_manager_->UnpinPage(last_pair_in_sibling.second, true);
          // std::cout << "发生了借兄弟(够借)" << last_pair_in_sibling.second << std::endl;
        } else {
          MappingType last_pair_in_sibling;
          auto sibling_leaf_page = reinterpret_cast<LeafPage *>(checked_sibling_page);
          auto current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
          last_pair_in_sibling.first = sibling_leaf_page->KeyAt(sibling_leaf_page->GetSize() - 1);
          last_pair_in_sibling.second = sibling_leaf_page->ValueAt(sibling_leaf_page->GetSize() - 1);
          current_leaf_page->InsertInLeaf(last_pair_in_sibling.first, last_pair_in_sibling.second, comparator_);
          sibling_leaf_page->Delete(last_pair_in_sibling.first, comparator_);
          // 这里只需把key怼到父节点就够了，不需要交换，所以变量用不到了
          parent_internal_page->SwapKeyAtValue(last_pair_in_sibling.first, current_page->GetPageId());
        }
      } else {  //  兄弟N'为右兄弟，左借右
        if (!current_page->IsLeafPage()) {
          std::pair<KeyType, page_id_t> first_pair_in_sibling;
          auto sibling_internal_page = reinterpret_cast<InternalPage *>(checked_sibling_page);
          auto current_internal_page = reinterpret_cast<InternalPage *>(current_page);
          first_pair_in_sibling.first = sibling_internal_page->KeyAt(1);
          first_pair_in_sibling.second = sibling_internal_page->ValueAt(0);
          sibling_internal_page->Delete(first_pair_in_sibling.second);
          parent_internal_page->SwapKeyAtValue(first_pair_in_sibling.first, sibling[checked_sibling_index].second);
          current_internal_page->InsertInInternal(first_pair_in_sibling.first, first_pair_in_sibling.second,
                                                  comparator_);  // 插到最后
          // printf("first_pair's value:%d\n",first_pair_in_sibling.second);
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(first_pair_in_sibling.second))
              ->SetParentPageId(current_internal_page->GetPageId());
          buffer_pool_manager_->UnpinPage(first_pair_in_sibling.second, true);

        } else {
          MappingType first_pair_in_sibling;
          auto sibling_leaf_page = reinterpret_cast<LeafPage *>(checked_sibling_page);
          auto current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
          first_pair_in_sibling.first = sibling_leaf_page->KeyAt(0);
          first_pair_in_sibling.second = sibling_leaf_page->ValueAt(0);
          current_leaf_page->InsertInLeaf(first_pair_in_sibling.first, first_pair_in_sibling.second, comparator_);  //
          sibling_leaf_page->Delete(first_pair_in_sibling.first, comparator_);
          parent_internal_page->SwapKeyAtValue(first_pair_in_sibling.first, sibling[checked_sibling_index].second);
        }
      }

      // std::cout << "兄弟充足，向兄弟借kv完成" << std::endl;
    }

    buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(sibling[checked_sibling_index].second, true);
  }
  // buffer_pool_manager_->UnpinPage(current_page->GetPageId(), true);
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  //不上锁也没关系，这个函数不要求实现并发
  page_id_t target_page_id= root_page_id_;
  auto begin_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(target_page_id));
  while(!begin_page->IsLeafPage()) {
    auto child_page_id = reinterpret_cast<InternalPage*>(begin_page)->ValueAt(0);
    begin_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(child_page_id));
    buffer_pool_manager_->UnpinPage(target_page_id, false);
    target_page_id = child_page_id;
  }
  auto begin_leaf_page = reinterpret_cast<LeafPage*>(begin_page);
  IndexIterator<KeyType, ValueType, KeyComparator> index_iterator(begin_leaf_page, comparator_, buffer_pool_manager_,
                                                                  begin_leaf_page->KeyAt(0));
  buffer_pool_manager_->UnpinPage(begin_page->GetPageId(), false);
  return index_iterator;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto begin_leaf_page = FindLeafPage(key);
  begin_leaf_page->GetLatch().WUnlock();
  IndexIterator<KeyType, ValueType, KeyComparator> index_iterator(begin_leaf_page, comparator_, buffer_pool_manager_,
                                                                  key);
  
  buffer_pool_manager_->UnpinPage(begin_leaf_page->GetPageId(), false);
  return index_iterator;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  IndexIterator<KeyType, ValueType, KeyComparator> index_iterator(comparator_);
  return index_iterator;
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
