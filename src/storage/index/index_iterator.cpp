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
INDEXITERATOR_TYPE::IndexIterator(LeafPage* begin_leaf, KeyComparator& comparator, BufferPoolManager* bpm):
comparator_(comparator),buffer_pool_manager_(bpm) {
  head_ = new LeafNode(begin_leaf->array_, begin_leaf->GetSize());
  LeafNode* cur_node = head_;
  page_id_t next_page_id = begin_leaf->GetNextPageId();
  
  while (next_page_id != INVALID_PAGE_ID) {
    auto current_page = reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(next_page_id));
    auto p =new LeafNode(current_page->array_, current_page->GetSize());
    cur_node->next_node_ = p;
    cur_node = cur_node->next_node_;
    next_page_id = current_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
  }
  tail_ = cur_node;
  cur_node->next_node_ = nullptr;
  current_leaf_node_ = head_;
  index_in_current_page_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(KeyComparator &comparator):comparator_(comparator),
current_leaf_node_(nullptr),index_in_current_page_(0) {}


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
  auto cur_node = head_;
  while(cur_node) {
    auto next_node = cur_node->next_node_;
    delete cur_node;
    cur_node = next_node;
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
   return current_leaf_node_ == nullptr || 
   (current_leaf_node_ == tail_ && index_in_current_page_ >= current_leaf_node_->size_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
  return current_leaf_node_->array_[index_in_current_page_];
 }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 

  index_in_current_page_++;
  if (index_in_current_page_ >= current_leaf_node_->size_) {
    current_leaf_node_ = current_leaf_node_->next_node_;
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
