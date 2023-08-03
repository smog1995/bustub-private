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
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
namespace bustub {
#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(KeyComparator &comparator);
  IndexIterator(LeafPage *begin_leaf, KeyComparator &comparator, BufferPoolManager *bpm, const KeyType &key);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return index_in_current_page_ == itr.index_in_current_page_ && current_leaf_page_ == itr.current_leaf_page_;
  };

  auto operator!=(const IndexIterator &itr) const -> bool {
    return index_in_current_page_ != itr.index_in_current_page_ || current_leaf_page_ != itr.current_leaf_page_;
  }

 private:
  KeyComparator comparator_;
  BufferPoolManager *buffer_pool_manager_;
  LeafPage *current_leaf_page_;
  int index_in_current_page_;

  // add your own private member variables here
};

}  // namespace bustub
