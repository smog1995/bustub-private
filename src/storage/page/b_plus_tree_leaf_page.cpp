//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <map>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType& key, std::vector<ValueType> *result, KeyComparator& comparator) -> bool {
  std::cout << " op Find of LeafPage:";
  for (int index = 0; index < GetSize(); index++) {
    std::cout << " compare key " << key << "vs" << array_[index].first << " res: " << comparator(key, array_[index].first) << std::endl;
    if(comparator(key, array_[index].first) == 0) {
      
      for (int res_index = index; res_index < GetSize() && comparator(key, array_[res_index].first) == 0; res_index++) {
        result->emplace_back(array_[res_index].second);
      }
      return true;
    }
  }
  return false;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertInLeaf(const KeyType &key, const ValueType &value, KeyComparator &comparator)
    -> bool {
  int index;
  std::cout << "current page:" << GetPageId() << "Size:" << GetSize() << " Insert:" << key ;
  if (GetSize() == 0) {
    std::cout << " success, now it has " << GetSize() << " pair." << std::endl;
    array_[0].first = key;
    array_[0].second = value;
    IncreaseSize(1);
    return true;
  }
  for (index = 0; index < GetSize(); index++) {
    if (comparator(key, array_[index].first) < 0) {
      break;
    }
    if (comparator(key, array_[index].first) == 0) {
      std::cout << " failed, now it has " << GetSize() << " pair" << std::endl;
      // 含有相同key的元素
      return false;
    }
  }
  for (int move = GetSize() - 1; move >= index; move--) {
    array_[move + 1] = array_[move];
  }
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
  std::cout << " success, now it has " << GetSize() << " pair." << std::endl;
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyToArray(MappingType *array) {
  for (int index = 0; index < GetSize(); index++) {
    array[index] = array_[index];
  }
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertArray(MappingType *array, int low, int high) {
  // memset(array_, 0, sizeof(MappingType) * GetMaxSize());
  for (int index = low; index <= high; index++) {
    array_[index - low] = array[index];
  }
  SetSize(high - low + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, KeyComparator &comparator) {
  int index;
  for (index = 0; index < GetSize(); index++) {
    if (comparator(array_[index].first, key) == 0) {
      for (int move = index; move < GetSize() - 1; move++) {
        array_[move] = array_[move + 1];
      }
    }
    DecrementSize();
  }
  // don't worry about there is only one pair in page cause the page will be delete.
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeInLeaf(MappingType *array, int array_size) {
  for (int index = GetSize(); index < GetSize() + array_size; index++) {
    array_[index] = array[index - GetSize()];
  }
  IncreaseSize(array_size);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
