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
  head = new LeafNode(begin_leaf->array_, current_page->GetSize());
  LeafPage* cur_node = head;
  page_id_t next_page_id = current_page->GetNextPageId();
  while (next_page_id != INVALID_PAGE_ID) {
    current_page = reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(next_page_id));
    auto p =new LeafNode(current_page->array_, current_page->GetSize());
    cur_node->next = p;
    cur_node = cur_node->next;
    next_page_id = p->GetNextPageId();
  }
}


INDEXITERATOR_TYPE::LinkList::LinkList() {
  
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
  auto cur_node = head;
  while(cur_node != nullptr) {
    auto next_node = cur_node->next;
    delete cur_node;
    cur_node = next_node;
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { throw std::runtime_error("unimplemented"); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
  if (index_in_current_page_ >= current_leaf_page_->GetSize()) {
    current_leaf_page_ = reinterpret_cast<BPlusTreeLeafPage*> ()
  }
 }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
