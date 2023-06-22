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

LRUKReplacer::~LRUKReplacer() {
  //这是用于第三个模块buffermanager使用的，当一个节点始终都处于不可驱逐转态，那他不会存在于两条队列中也就不能被释放
  for (std::unordered_map<frame_id_t, Node *>::iterator it = map_.begin(); it != map_.end(); it++) {
    if(it->second != nullptr)
      delete it->second;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (inflist_.size()) {
    *frame_id = inflist_.Dequeue();
  } else if (klist_.size()) {
    *frame_id = klist_.Dequeue();
  } else {
    return false;
  }
  std::lock_guard<std::mutex> lock(latch_);
  map_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  Node *x;
  timestamp_lock.lock();
  current_timestamp_++;
  timestamp_lock.unlock();
  if ((size_t)frame_id > replacer_size_ || frame_id < 0) throw "invalid frame_id!";
  if (!map_.count(frame_id)) {
    x = new Node(frame_id);
    x->accesses_++;
    map_[frame_id] = x;
  } else {
    x = map_[frame_id];
    x->accesses_++;
    x->timestamp_ = current_timestamp_;
    if (!x->evictable_) {
      // 如果是不可驱逐的，则直接结束
      return;
    } else if (x->accesses_ == k_) {
      // 从inflist_移到klist_
      inflist_.RemoveX(x);
      klist_.Enqueue(x);
    } else if (x->accesses_ < k_) {
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
  if ((size_t)frame_id > replacer_size_ || frame_id < 0) throw "invalid frame_id!";
  Node *x = map_[frame_id];
  if (set_evictable == x->evictable_) {
    return;
  } else if (set_evictable) {  // 如果是设置为可驱逐，则将该页框放入可驱逐队列
    // 如果访问次数达到k,直接插入klist_
    if (x->accesses_ >= k_) {
      klist_.InsertX(x);
    } else {
      inflist_.InsertX(x);  // 否则插入inflist_
    }
  } else {  // 如果是设置为不可驱逐，则需要移出可驱逐队列
    if (x->accesses_ >= k_)
      klist_.RemoveX(x);
    else
      inflist_.RemoveX(x);
  }
  x->evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (!map_.count(frame_id)) return;
  Node *x = map_[frame_id];
  if (!x->evictable_) throw "invalid frame_id!";
  map_.erase(frame_id);
  if (x->accesses_ >= k_)
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
/*
LRUKReplacer::DoubleList::~DoubleList() {
  Node *p = head_->next_;
  while (p) {
    head_->next_ = p->next_;
    delete p;
    p = head_->next_;
  }
  delete head_;
}
*/
LRUKReplacer::DoubleList::~DoubleList() {
  delete head_;
  delete tail_;
}
void LRUKReplacer::DoubleList::RemoveX(Node *x) {
  std::lock_guard<std::mutex> lock(latch_);
  if (!x || !x->next_) return;
  x->next_->pre_ = x->pre_;
  x->pre_->next_ = x->next_;
  size_--;
}
void LRUKReplacer::DoubleList::Enqueue(Node *x) {
  std::lock_guard<std::mutex> lock(latch_);
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
      if (x->timestamp_ < p->timestamp_) {
        std::lock_guard<std::mutex> lock(latch_);
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
  // std::lock_guard<std::mutex> lock(latch_);
  if (!size()) return 0;
  Node *p = head_->next_;
  frame_id_t frame_id = p->frame_id_;
  RemoveX(p);
  delete p;
  return frame_id;
}

}  // namespace bustub
