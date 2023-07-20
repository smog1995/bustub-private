//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <iostream>
#include <sstream>
#include <utility>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index + 1].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
  IncreaseSize(1);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertArray(const MappingType *array, int low, int high) {
  memset(array_, 0, sizeof(MappingType) * GetMaxSize());
  for (int index = low; index <= high; index++) {
    array_[index - low] = array[index];
  }
  SetSize(high - low + 1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyToArray(MappingType *array) {
  for (int index = 0; index < GetSize(); index++) {
    array[index] = array_[index];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertInInternal(const KeyType &key, const ValueType &value,
                                                      KeyComparator &comparator) {
  int index;
  for (index = 1; index < GetSize(); index++) {
    if (comparator(key, array_[index].first) < 0) {
      for (int move = GetSize() - 1; move >= index; move++) {
        array_[move + 1] = array_[move];
      }
      array_[index].first = key;
      array_[index].second = value;
      IncreaseSize(1);
      break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSibling(MappingType *result, const ValueType &current_pageid) {
  for (int index = 0; index < GetSize(); index++) {
    if (current_pageid == array_[index].second) {
      if (index - 1 >= 0) {
        result[0] = array_[index - 1];
      }
      if (index + 1 < GetSize()) {
        result[1] = array_[index + 1];
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const ValueType &value) {
  for (int index = 1; index < GetSize(); index++) {
    if (value == array_[index].second) {
      for (int move = index; move < GetSize() - 1; move++) {
        array_[move] = array_[move + 1];
      }
      break;
    }
  }
  DecrementSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeInInternal(const KeyType &parentKey, MappingType *array, int array_size) {
  int size = GetSize();
  array_[size].first = parentKey;
  array_[size].second = array[0].second;
  for (int index = size + 1; index < size + array_size; index++) {
    array_[index] = array[index - size];
  }
  IncreaseSize(array_size);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
