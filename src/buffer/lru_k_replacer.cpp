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
#include <iostream>
#include "shared_mutex"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // std::cout << "size:" << replacer_size_ << std::endl;
}

LRUKReplacer::~LRUKReplacer() {
  for (auto &it : map_) {
    delete it.second;
  }
  // std::cout << "LRU析构,map中的元素为" << map_.size() << std::endl;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (inflist_.Size() > 0) {
    *frame_id = inflist_.Dequeue();
    // std::cout << "inf队列：";
    // inflist_.PrintList();
    // std::cout << "k队列：";
    // klist_.PrintList();
  } else if (klist_.Size() > 0) {
    *frame_id = klist_.Dequeue();
    // std::cout << "inf队列：";
    // inflist_.PrintList();
    // std::cout << "k队列：";
    // klist_.PrintList();
  } else {
    // std::cout << "evict失败，无可驱逐的页" << std::endl;
    return false;
  }
  map_.erase(*frame_id);
  // std::cout << "evict: " << *frame_id << std::endl;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "recordaccess:" << frame_id << "结果：";
  Node *x = nullptr;
  current_timestamp_++;
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    // std::cout << " 无效页id!" << std::endl;
    throw "invalid frame_id!";
  }
  if (map_.count(frame_id) == 0) {
    x = new Node(frame_id);
    x->accesses_++;
    x->timestamp_ = current_timestamp_;
    map_[frame_id] = x;
    // std::cout << " 该页为初次进lruK" << std::endl;
  } else {
    x = map_[frame_id];
    x->accesses_++;
    // std::cout << "该页不为初次进lruk";
    if (!x->evictable_) {
      // std::cout << " 但被pin住，不进驱逐队列" << std::endl;
      if (x->accesses_ >= k_) {
        x->timestamp_ = current_timestamp_;
      }
      // 如果是不可驱逐的，则直接结束
      return;
    }
    if (x->accesses_ == k_) {
      // 从inflist_移到klist_
      x->timestamp_ = current_timestamp_;
      // std::cout << " 等于k次，直接移出inf队列，转到klist队列";
      inflist_.RemoveX(x);
      klist_.Enqueue(x);
    } else if (x->accesses_ < k_) {
      // 将其移到inflist_队尾
      // std::cout << "小于k次，放到inf队列即挪到inf队尾";
      inflist_.RemoveX(x);
      inflist_.Enqueue(x);
    } else {
      // 将其移到klist_队尾
      x->timestamp_ = current_timestamp_;
      // std::cout << "大于k次，放到k队列即挪到k队尾";
      klist_.RemoveX(x);
      klist_.Enqueue(x);
    }
  }
  // std::cout << std::endl;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "setevictable:" << frame_id << "set as:" << set_evictable;
  if (map_.count(frame_id) == 0) {
    // std::cout << "this frame is not found." << std::endl;
    return;
  }
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    // std::cout << "invalid frame_id!" << std::endl;
    throw "invalid frame_id!";
  }
  Node *x = map_[frame_id];
  if (set_evictable == x->evictable_) {
    // std::cout << "set the same as last." << std::endl;
    return;
  }
  if (set_evictable) {  // 如果是设置为可驱逐，则将该页框放入可驱逐队列
    // std::cout << "<<k's value is:" << k_ << ">>";
    // 如果访问次数达到k,直接插入klist_
    if (x->accesses_ >= k_) {
      // std::cout << "accesses larger or equal to k，insert into klist.";
      klist_.InsertX(x);
    } else {
      // std::cout << "accesses smaller than k，insert into inflist.";
      inflist_.InsertX(x);  // 否则插入inflist_
    }
  } else {  // 如果是设置为不可驱逐，则需要移出可驱逐队列
    // std::cout << " set as not_evictable ";
    if (x->accesses_ >= k_) {
      // std::cout << "accesses larger or equal to k， remove from klist.";
      klist_.RemoveX(x);
    } else {
      // std::cout << "accesses smaller than k， remove from inflist.";
      inflist_.RemoveX(x);
    }
  }
  x->evictable_ = set_evictable;
  // std::cout << std::endl;
}
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "remove:" << frame_id;
  if (map_.count(frame_id) == 0) {
    // std::cout << "this frame is not found." << std::endl;
    return;
  }
  Node *x = map_[frame_id];
  if (!x->evictable_) {
    // std::cout << "the frame is not evictable" << std::endl;
    throw "invalid frame_id!";
  }
  map_.erase(frame_id);
  if (x->accesses_ >= k_) {
    klist_.RemoveX(x);
  } else {
    inflist_.RemoveX(x);
  }
  delete x;
}

auto LRUKReplacer::Size() -> size_t { return klist_.Size() + inflist_.Size(); }

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
  if (x == nullptr || x->next_ == nullptr) {
    return;
  }
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
  if (size_ == 0) {
    Enqueue(x);
    return;
  }  // 否则比较时间戳，插入合适的位置
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
auto LRUKReplacer::DoubleList::Dequeue() -> size_t {
  // 无需检测队列是否为空，因为remove函数会检测删除的节点是否有next_指针为空的情况，有表示该节点为tail_
  Node *p = head_->next_;
  frame_id_t frame_id = p->frame_id_;
  RemoveX(p);
  delete p;
  return frame_id;
}
auto LRUKReplacer::DoubleList::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return size_;
}

void LRUKReplacer::DoubleList::PrintList() const {
  Node *p = head_->next_;
  while (p != nullptr && p != tail_) {
    std::cout << p->frame_id_ << " ";
    p = p->next_;
  }
  // std::cout << "队列元素数量:" << size_ << std::endl;
}
}  // namespace bustub
