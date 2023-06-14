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
  if (InfList.Size()) {
    *frame_id = InfList.Dequeue();
  } else if (KList.Size()) {
    *frame_id = KList.Dequeue();
  } else {
    return false;
  }
  map.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  Node* x;
  current_timestamp_++;
  if ((size_t)frame_id > replacer_size_|| frame_id < 1) throw "invalid frame_id!";
  if (!map.count(frame_id)) {
    x = new Node(frame_id);
    x->IncrementAccesses();
    map[frame_id] = x;
  } else {
    x = map[frame_id];
    x->IncrementAccesses();
    x->SetTimeStamp(current_timestamp_);
    if (!x->IsEvictable()) {
      // 如果是不可驱逐的，则不在驱逐队列内
      return;
    } else if (x->GetAccesses() == k_) {
      // 从InfList移到KList
      InfList.RemoveX(x);
      KList.Enqueue(x);
    } else if (x->GetAccesses() < k_) {
      // 将其移到Inflist队尾
      InfList.RemoveX(x);
      InfList.Enqueue(x);
    } else {
      // 将其移到Klist队尾
      KList.RemoveX(x);
      KList.Enqueue(x);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (!map.count(frame_id)) return;
  if ((size_t)frame_id > replacer_size_|| frame_id < 1) throw "invalid frame_id!";
  Node* x = map[frame_id];
  if (set_evictable == x->IsEvictable()) {
    return;
  } else if (set_evictable) {  // 如果是设置为可驱逐，则将该页框放入可驱逐队列
    // 如果访问次数达到k,直接插入KList
    if (x->GetAccesses() >= k_) {
      KList.InsertX(x);
    } else {
      InfList.InsertX(x);  // 否则插入InfList
    }
  } else {  // 如果是设置为不可驱逐，则需要移出可驱逐队列
    if (x->GetAccesses() >= k_) KList.RemoveX(x);
    else
      InfList.RemoveX(x);
  }
  x->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (!map.count(frame_id)) return;
  Node* x = map[frame_id];
  if (!x->IsEvictable()) throw "invalid frame_id!";
  map.erase(frame_id);
  if (x->GetAccesses() >= k_) KList.RemoveX(x);
  else  InfList.RemoveX(x);
  delete x;
}

auto LRUKReplacer::Size() -> size_t { return KList.Size()+InfList.Size(); }


//===--------------------------------------------------------------------===//
// DoubleList
//===--------------------------------------------------------------------===//
LRUKReplacer::DoubleList::DoubleList() {
  tail = new Node(0);
  head = new Node(0);
  tail->pre = head;
  head->next = tail;
  size = 0;
}
LRUKReplacer::DoubleList::~DoubleList() {
  Node* p = head->next;
  while (p) {
    head->next = p->next;
    delete p;
    p = head->next;
  }
  delete head;
}
void LRUKReplacer::DoubleList::RemoveX(Node* x) {
  if (!x) return;
  x->next->pre = x->pre;
  x->pre->next = x->next;
  size--;
}
void LRUKReplacer::DoubleList::Enqueue(Node* x) {
  x->next = tail;
  x->pre = tail->pre;
  tail->pre->next = x;
  tail->pre = x;
  size++;
}
void LRUKReplacer::DoubleList::InsertX(Node* x) {
  // 假设链表为空，直接插入即可
  if (!Size()) {
    Enqueue(x);
    return;
  } else {  // 否则比较时间戳，插入合适的位置
    Node* p = head->next;
    while (p != tail) {
      if (x->GetTimestamp() < p->GetTimestamp()) {
        x->pre = p->pre;
        x->next = p;
        p->pre->next = x;
        p->pre = x;
        size++;
        return;
      }
      p = p->next;
    }
    // 如果到了队尾还没插入，说明时间戳为最大值，直接插入队尾
    Enqueue(x);
  }
}
auto LRUKReplacer::DoubleList::Dequeue() -> size_t {
  if (!Size()) return 0;
  Node* p = head->next;
  frame_id_t frame_id = p->GetFrameid();
  RemoveX(p);
  delete p;
  return frame_id;
}

}  // namespace bustub

