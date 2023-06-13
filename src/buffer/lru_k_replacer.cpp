//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <unordered_map>
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    

    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    Node* x;
    if(!map.count(frame_id))
    {
        x=new Node(frame_id);
        map[frame_id]=x;
        return ;
    }
    x=map[frame_id];
    x->IncrementAccesses();
    if(x->GetAccesses()>=k_){
        x->SetTimeStamp(current_timestamp_);
        DList.Enqueue(x);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    Node* x
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }


//===--------------------------------------------------------------------===//
// DoubleList
//===--------------------------------------------------------------------===//
LRUKReplacer::DoubleList::DoubleList(){
    tail=new Node(0);
    head=new Node(0);
    tail->pre=head;
    head->next=tail;
    size=0;
}
void LRUKReplacer::DoubleList::RemoveX(Node* x){
    if(!x) return ;
    x->next->pre=x->pre;
    x->pre->next=x->next;
    delete x;
    size--;
}
void LRUKReplacer::DoubleList::Enqueue(Node* x){
    x->next=head->next;
    x->pre=head;
    head->next->pre=x;
    head->next=x;
    size++;
}
void LRUKReplacer::DoubleList::Dequeue(){
    if(tail->pre=head) return ;
    Node* p=tail->pre;
    RemoveX(p);
}




}  // namespace bustub
