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
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (inflist_.size()) {
    *frame_id = inflist_.Dequeue();
  } else if (klist_.size()) {
    *frame_id = klist_.Dequeue();
  } else {
    return false;
  }
  map_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  Node *x;
  current_timestamp_++;
  if ((size_t)frame_id > replacer_size_ || frame_id < 1) throw "invalid frame_id!";
  if (!map_.count(frame_id)) {
    x = new Node(frame_id);
    x->IncrementAccesses();
    map_[frame_id] = x;
  } else {
    x = map_[frame_id];
    x->IncrementAccesses();
    x->SetTimeStamp(current_timestamp_);
    if (!x->IsEvictable()) {
      // 如果是不可驱逐的，则不在驱逐队列内
      return;
    } else if (x->GetAccesses() == k_) {
      // 从inflist_移到klist_
      inflist_.RemoveX(x);
      klist_.Enqueue(x);
    } else if (x->GetAccesses() < k_) {
      // 将其移到inflist_队尾
      inflist_.RemoveX(x);
      inflist_.Enqueue(x);
    } else {
      // 将其移到klist_队尾
      klist_.RemoveX(x);
      klist_.Enqueue(x);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (!map_.count(frame_id)) return;
  if ((size_t)frame_id > replacer_size_ || frame_id < 1) throw "invalid frame_id!";
  Node *x = map_[frame_id];
  if (set_evictable == x->IsEvictable()) {
    return;
  } else if (set_evictable) {  // 如果是设置为可驱逐，则将该页框放入可驱逐队列
    // 如果访问次数达到k,直接插入klist_
    if (x->GetAccesses() >= k_) {
      klist_.InsertX(x);
    } else {
      inflist_.InsertX(x);  // 否则插入inflist_
    }
  } else {  // 如果是设置为不可驱逐，则需要移出可驱逐队列
    if (x->GetAccesses() >= k_)
      klist_.RemoveX(x);
    else
      inflist_.RemoveX(x);
  }
  x->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (!map_.count(frame_id)) return;
  Node *x = map_[frame_id];
  if (!x->IsEvictable()) throw "invalid frame_id!";
  map_.erase(frame_id);
  if (x->GetAccesses() >= k_)
    klist_.RemoveX(x);
  else
    inflist_.RemoveX(x);
  delete x;
}

auto LRUKReplacer::Size() -> size_t { return klist_.size() + inflist_.size(); }

//===--------------------------------------------------------------------===//
// DoubleList
//===--------------------------------------------------------------------===//
LRUKReplacer::DoubleList::DoubleList() {
  tail_ = new Node(0);
  head_ = new Node(0);
  tail_->pre_ = head_;
  head_->next_ = tail_;
  size_ = 0;
}
LRUKReplacer::DoubleList::~DoubleList() {
  Node *p = head_->next_;
  while (p) {
    head_->next_ = p->next_;
    delete p;
    p = head_->next_;
  }
  delete head_;
}
void LRUKReplacer::DoubleList::RemoveX(Node *x) {
  if (!x) return;
  x->next_->pre_ = x->pre_;
  x->pre_->next_ = x->next_;
  size_--;
}
void LRUKReplacer::DoubleList::Enqueue(Node *x) {
  x->next_ = tail_;
  x->pre_ = tail_->pre_;
  tail_->pre_->next_ = x;
  tail_->pre_ = x;
  size_++;
}
void LRUKReplacer::DoubleList::InsertX(Node *x) {
  // 假设链表为空，直接插入即可
  if (!size()) {
    Enqueue(x);
    return;
  } else {  // 否则比较时间戳，插入合适的位置
    Node *p = head_->next_;
    while (p != tail_) {
      if (x->GetTimestamp() < p->GetTimestamp()) {
        x->pre_ = p->pre_;
        x->next_ = p;
        p->pre_->next_ = x;
        p->pre_ = x;
        size_++;
        return;
      }
      p = p->next_;
    }
    // 如果到了队尾还没插入，说明时间戳为最大值，直接插入队尾
    Enqueue(x);
  }
}
auto LRUKReplacer::DoubleList::Dequeue() -> size_t {
  if (!size()) return 0;
  Node *p = head_->next_;
  frame_id_t frame_id = p->GetFrameid();
  RemoveX(p);
  delete p;
  return frame_id;
}

}  // namespace bustub
