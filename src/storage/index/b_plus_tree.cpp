#include <memory>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "type/value.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  leaf_min_size_ = ceil(static_cast<double>(leaf_max_size_ - 1) / 2);
  internal_min_size_ = ceil(static_cast<double>(internal_max_size_) / 2);
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
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  auto target_leaf_page = FindLeafPage(root_page_id_, key);
  return target_leaf_page->Find(key, result, comparator_);
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
auto BPLUSTREE_TYPE::FindLeafPage(page_id_t root_page, const KeyType &key) const -> LeafPage * {
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page));
  if (page->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(root_page, false);
    return reinterpret_cast<LeafPage *>(page);
  }
  LeafPage *result = nullptr;
  auto root = reinterpret_cast<InternalPage *>(page);
  for (int index = 1; index < root->GetSize(); index++) {
    std::cout << "在这里找页：" << key << ":" << root->KeyAt(index) << comparator_(key, root->KeyAt(index)) <<std::endl;
    int compare_result = comparator_(key, root->KeyAt(index));
    if ((compare_result < 0 && index == 1) || (compare_result >= 0 && index == root->GetSize() - 1) ||
      (compare_result >= 0 && comparator_(key, root->KeyAt(index + 1)) < 0)) {
        int res_index = (index == 1 && compare_result < 0) ? index - 1 : index;
      page_id_t child_pageid = root->ValueAt(res_index);
      auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_pageid));
      if (child_page->IsLeafPage()) {
        result = reinterpret_cast<LeafPage *>(child_page);
      } else {
        result = FindLeafPage(root->ValueAt(res_index), key);
      }
      buffer_pool_manager_->UnpinPage(child_pageid, false);
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(root_page, false);
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertArrayHelper(std::pair<KeyType, page_id_t> *array, int array_size, const KeyType& key, page_id_t value) {
  int index;
  for (index = 1; index < array_size; index ++) {
    if(comparator_(key, array[index].first) < 0) {
      break;
    }
  }
  for (int move = array_size - 1; move >= index; move--) {
    array[move + 1] = array[move];
  }
  array[index].first = key;
  array[index].second = value;
}

  // 设置child的父亲节点，如果在数组左半边的说明父亲为左边节点，否则父亲为右边新创建的节点
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetNewParentPageId(std::pair<KeyType, page_id_t> *array, int array_size, BPlusTreePage* child,
                                        InternalPage* left_parent, InternalPage* right_parent) {
  for (int index = 0; index <= array_size; index++) {
    if(child->GetPageId() == array[index].second) {
      child->SetParentPageId(left_parent->GetPageId());
      return;
    }
  }
  child->SetParentPageId(right_parent->GetPageId());
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage* left, const KeyType &key, BPlusTreePage* right) {
  std::cout << "insertInParent:" << "left pageid:" << left->GetPageId() << " right pageid:" << right->GetPageId() << std::endl; 
  // 一种情况是叶子节点为根节点已满，需要分裂叶节点，同时产生一个新的内部节点作为根节点
  InternalPage *new_internal_page;
  if (left->IsRootPage()) {
    new_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_));
    new_internal_page->Init(root_page_id_, -1, internal_max_size_);
    new_internal_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_internal_page->SetKeyAt(1, key);
    new_internal_page->SetValueAt(0, left->GetPageId());
    new_internal_page->SetValueAt(1, right->GetPageId());
    left->SetParentPageId(root_page_id_);
    right->SetParentPageId(root_page_id_);
    std::cout << "tree need generate new root " << std::endl;
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {  //第二种是父节点是内部节点，直接插入；
    std::cout << "need insert in internal" <<std::endl;
    page_id_t new_internal_pageid;
    auto parent_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left->GetParentPageId()));
    if (!parent_internal_page->InsertInInternal(key, right->GetPageId(), comparator_)) { // 同时若节点已满需要分裂内部节点同时插入父节点的父节点
      std::pair<KeyType, page_id_t> array[internal_max_size_ + 1];
      parent_internal_page->CopyToArray(array);
      InsertArrayHelper(array, internal_max_size_, key, right->GetPageId());
      new_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_internal_pageid));
      new_internal_page->Init(new_internal_pageid, -1, internal_max_size_);
      new_internal_page->SetPageType(IndexPageType::INTERNAL_PAGE);
      KeyType mid_internal_key = array[internal_max_size_ / 2 + 1].first;
      array[internal_max_size_ / 2 + 1].first.SetFromInteger(0); // 数组中这个位置的中间值会放到父节点中
      parent_internal_page->InsertArray(array, 0, internal_max_size_ / 2 );
      new_internal_page->InsertArray(array, internal_max_size_ / 2 + 1, internal_max_size_);
      SetNewParentPageId(array, internal_max_size_ / 2, left, parent_internal_page, new_internal_page);
      SetNewParentPageId(array, internal_max_size_ / 2, right, parent_internal_page, new_internal_page);
      InsertInParent(reinterpret_cast<BPlusTreePage*>(parent_internal_page), mid_internal_key, reinterpret_cast<BPlusTreePage*>(new_internal_page));
      buffer_pool_manager_->UnpinPage(new_internal_pageid, true);
    } else {
      right->SetParentPageId(parent_internal_page->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
  }
}

/**
 * 每次生成新节点时，需要初始化，包括页id，父节点页id，节点类型更新，若为新的根节点还需要更新root_page_id_
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    auto new_root = reinterpret_cast<LeafPage*>(buffer_pool_manager_->NewPage(&root_page_id_));
    UpdateRootPageId(1);
    new_root->Init(root_page_id_, -1, leaf_max_size_);
    new_root->SetPageType(IndexPageType::LEAF_PAGE);
    new_root->InsertInLeaf(key, value, comparator_);
    std::cout << "root is empty" << std::endl;
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  auto target_leaf_page = FindLeafPage(root_page_id_, key);
  std::cout << "leaf page find success. page_id: " << target_leaf_page->GetPageId() << std::endl;
  if (!target_leaf_page->InsertInLeaf(key, value, comparator_)) {
    std::cout << "there is a duplicate key in the leaf page." << std::endl;
    return false;  // 已有重复键
  }
  // 如果插入后叶子满了，需要进行分裂 （n-1才是稳定态）
  if (target_leaf_page->GetSize() == leaf_max_size_) {
    std::cout << "the leaf page new spilt." << std::endl;
    page_id_t another_leaf_pageid;
    auto another_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&another_leaf_pageid));
    another_leaf_page->Init(another_leaf_pageid, -1, leaf_max_size_);
    MappingType array[leaf_max_size_];
    target_leaf_page->CopyToArray(array);
    KeyType min_key_in_newleaf = array[leaf_max_size_ / 2].first;
    target_leaf_page->InsertArray(array, 0, leaf_max_size_ / 2 - 1);
    target_leaf_page->SetNextPageId(another_leaf_pageid);
    another_leaf_page->InsertArray(array, leaf_max_size_ / 2, leaf_max_size_ - 1);
    InsertInParent(reinterpret_cast<BPlusTreePage*>(target_leaf_page), min_key_in_newleaf, reinterpret_cast<BPlusTreePage*>(another_leaf_page));
    buffer_pool_manager_->UnpinPage(another_leaf_pageid, true);
    buffer_pool_manager_->UnpinPage(target_leaf_page->GetPageId(), true);
  }
  return true;
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
  if (IsEmpty()) {
    return;
  }
  auto target_leaf_page = FindLeafPage(root_page_id_, key);
  DeleteEntry(target_leaf_page->GetPageId(), key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(page_id_t current_pageid, const KeyType &key, page_id_t value) {
  auto current_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_pageid));
  if (current_page->IsRootPage() && current_page->GetSize() - 1 == 1 &&
      !current_page->IsLeafPage()) {  // N为根节点且删除后只剩一个孩子节点(前提为非叶节点)
    auto target_internal_page = reinterpret_cast<InternalPage *>(current_page);
    target_internal_page->Delete(value);
    root_page_id_ =
        target_internal_page->ValueAt(0);  // 每次删除都是删除K和K的右侧指针,所以留下的总是左节点，即0号value
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(current_pageid, false);
    buffer_pool_manager_->DeletePage(current_pageid);
  } else if ((current_page->IsLeafPage() && current_page->GetSize() < leaf_min_size_) ||
             (!current_page->IsLeafPage() && current_page->GetSize() < internal_min_size_)) {
    auto parent_internal_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(current_page->GetParentPageId()));
    std::pair<KeyType, page_id_t> sibling[2];
    sibling[0].second = -1;
    sibling[1].second = -1;
    parent_internal_page->GetSibling(sibling, current_pageid);
    page_id_t checked_sibling_index = sibling[0].second == -1 ? 1 : 0;
    auto checked_sibling_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(sibling[checked_sibling_index].second));
    // 如果两个节点加起来的pair数量小于maxsize（减1是因为删除key操作还没执行）
    if (checked_sibling_page->GetSize() + current_page->GetSize() - 1 < checked_sibling_page->GetMaxSize()) { 
      if (checked_sibling_index == 1) {  // 节点(N)的兄弟(N')为右兄弟 （也就是N在N'前）,两指针交换
      // 这样做的目的是保持每次都只删除右指针,父节点删除指向右指针的键值对
        auto temp = checked_sibling_page;
        checked_sibling_page = current_page;
        current_page = temp;
      }
      if (!current_page->IsLeafPage()) {  // 非叶子节点，则把父节点的key和N中的所有指针拿到N'中
        auto sibling_internal_page = reinterpret_cast<InternalPage *>(checked_sibling_page);
        auto current_internal_page = reinterpret_cast<InternalPage *>(current_page);
        current_internal_page->Delete(value);
        std::pair<KeyType, page_id_t> array[current_internal_page->GetSize()];
        current_internal_page->CopyToArray(array);
        sibling_internal_page->MergeInInternal(sibling[checked_sibling_index].first, array,
                                               current_internal_page->GetSize());
      } else {  //叶子节点，则把N中所有K,V拿到N'中,还需注意把nextpageId改下
        auto sibling_leaf_page = reinterpret_cast<LeafPage *>(checked_sibling_page);
        auto current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
        current_leaf_page->Delete(key, comparator_);
        MappingType array[current_leaf_page->GetSize()];
        current_leaf_page->CopyToArray(array);
        sibling_leaf_page->MergeInLeaf(array, current_leaf_page->GetSize());
        sibling_leaf_page->SetNextPageId(current_leaf_page->GetNextPageId());
      }
      DeleteEntry(current_page->GetParentPageId(), sibling[checked_sibling_index].first, current_pageid);
    } else {  // 兄弟的pair数量充足，从兄弟借一个pair即可，无需分裂
      if (checked_sibling_index == 0) { //兄弟N‘为节点N的前一个节点 (向左兄弟借pair)，右借左
        if (!current_page->IsLeafPage()) {
          std::pair<KeyType, page_id_t> last_pair_in_sibling;
          auto sibling_internal_page = reinterpret_cast<InternalPage*>(checked_sibling_page);
          auto current_internal_page = reinterpret_cast<InternalPage*>(current_page);
          last_pair_in_sibling.first = sibling_internal_page->KeyAt(sibling_internal_page->GetSize() - 1);
          last_pair_in_sibling.second = sibling_internal_page->ValueAt(sibling_internal_page->GetSize() - 1);
          sibling_internal_page->Delete(last_pair_in_sibling.second);
          parent_internal_page->SwapKeyAtValue(last_pair_in_sibling.first, current_pageid);
          current_internal_page->InsertInFirst(value,last_pair_in_sibling.first);
        } else {
          MappingType last_pair_in_sibling;
          auto sibling_leaf_page = reinterpret_cast<LeafPage*>(checked_sibling_page);
          auto current_leaf_page = reinterpret_cast<LeafPage*>(current_page);
          last_pair_in_sibling.first = sibling_leaf_page->KeyAt(sibling_leaf_page->GetSize() - 1);
          last_pair_in_sibling.second = sibling_leaf_page->ValueAt(sibling_leaf_page->GetSize() - 1);
          current_leaf_page->InsertInLeaf(last_pair_in_sibling.first, last_pair_in_sibling.second, comparator_);
          sibling_leaf_page->Delete(last_pair_in_sibling.first, comparator_);
          //这里只需把key怼到父节点就够了，不需要交换，所以变量用不到了
          parent_internal_page->SwapKeyAtValue(last_pair_in_sibling.first, current_pageid);
        }
      } else {  //兄弟N'为右兄弟，左借右
        if (!current_page->IsLeafPage()) {
          std::pair<KeyType, page_id_t> last_pair_in_sibling;
          auto sibling_internal_page = reinterpret_cast<InternalPage*>(checked_sibling_page);
          auto current_internal_page = reinterpret_cast<InternalPage*>(current_page);
          last_pair_in_sibling.first = sibling_internal_page->KeyAt(1);
          last_pair_in_sibling.second = sibling_internal_page->ValueAt(0);
          sibling_internal_page->Delete(last_pair_in_sibling.second);
          parent_internal_page->SwapKeyAtValue(last_pair_in_sibling.first, sibling[checked_sibling_index].second);
          current_internal_page->InsertInInternal(last_pair_in_sibling.first, last_pair_in_sibling.second, comparator_); // 插到最后
        } else {
          MappingType last_pair_in_sibling;
          auto sibling_leaf_page = reinterpret_cast<LeafPage*>(checked_sibling_page);
          auto current_leaf_page = reinterpret_cast<LeafPage*>(current_page);
          last_pair_in_sibling.first = sibling_leaf_page->KeyAt(0);
          last_pair_in_sibling.second = sibling_leaf_page->ValueAt(0);
          current_leaf_page->InsertInLeaf(last_pair_in_sibling.first, last_pair_in_sibling.second, comparator_); //
          sibling_leaf_page->Delete(last_pair_in_sibling.first, comparator_);
          parent_internal_page->SwapKeyAtValue(last_pair_in_sibling.first, sibling[checked_sibling_index].second);
        }
      }
      buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(current_pageid, true);
    buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling[checked_sibling_index].second, true);
  }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
