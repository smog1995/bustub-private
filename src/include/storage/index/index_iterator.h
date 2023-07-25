//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"
namespace bustub {
#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>



INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(LeafPage* begin_leaf, KeyComparator& comparator, BufferPoolManager* bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { throw std::runtime_error("unimplemented"); }

  auto operator!=(const IndexIterator &itr) const -> bool { throw std::runtime_error("unimplemented"); }
  struct LeafNode{
    MappingType *array_;
    int size_;
    LeafNode* next_node_;
    LeafNode(MappingType *array, int size):array_(array),size_() {};
  }
  
 private:
  BPlusTreeLeafPage* current_leaf_page_;
  KeyComparator comparator_;
  BufferPoolManager *buffer_pool_manager_;
  LeafNode* head;
  // add your own private member variables here
};


}  // namespace bustub
