/**
 * index_iterator.cpp
 */
#include <cassert>
#include <memory>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *begin_leaf, KeyComparator &comparator, BufferPoolManager *bpm,
                                  const KeyType &key)
    : comparator_(comparator), buffer_pool_manager_(bpm) {
  current_leaf_page_ = begin_leaf;
  index_in_current_page_ = 0;
  KeyType invalid_key;
  invalid_key.SetFromInteger(-1);
  if (comparator_(key, invalid_key) != 0) {
    while (comparator_(key, current_leaf_page_->array_[index_in_current_page_].first) != 0) {
      index_in_current_page_++;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(KeyComparator &comparator)
    : comparator_(comparator), current_leaf_page_(nullptr), index_in_current_page_(0) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (current_leaf_page_ != nullptr) {
    current_leaf_page_->GetLatch().RUnlock();
    buffer_pool_manager_->UnpinPage(current_leaf_page_->GetPageId(), false);
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return current_leaf_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  return current_leaf_page_->array_[index_in_current_page_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_in_current_page_++;
  if (index_in_current_page_ >= current_leaf_page_->GetSize()) {
    page_id_t next_page_id = current_leaf_page_->GetNextPageId();
    current_leaf_page_->GetLatch().RUnlock();
    buffer_pool_manager_->UnpinPage(current_leaf_page_->GetPageId(), false);
    if (next_page_id != INVALID_PAGE_ID) {
      current_leaf_page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_page_id));
      current_leaf_page_->GetLatch().RLock();
    } else {
      current_leaf_page_ = nullptr;
    }
    index_in_current_page_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
