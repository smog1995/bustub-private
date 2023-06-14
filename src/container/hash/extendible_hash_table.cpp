//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t dir_index = IndexOf(key);

  return dir_[dir_index]->Remove(key);
}
size_t pow(size_t a, size_t b) {
  size_t ans = 1;
  for (size_t i = 0; i < b; i++)
    ans *= a;
  return ans;
}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // 一开始时，桶的个数为0，需要创建
  if (dir_.empty()) {
    std::shared_ptr<Bucket> p(new Bucket(bucket_size_, 0));
    dir_.push_back(p);
  }
  size_t dir_index = IndexOf(key);
  if (!dir_[dir_index]->Insert(key, value)) {  // 进行插入操作，当插入失败时
    int flag = 0;  // 用于标志是否全局深度等于本地深度
    // 全局深度等于本地深度，此时需要目录增长，指针重分配
    if (GetLocalDepth(dir_index) == GetGlobalDepth()) {
      flag = 1;
      //  std::cout<<"aaaa"<<std::endl;
      // 首先让哈希表目录长度翻倍
      IncrementGlobalDepth();
      // 分配目录表
      size_t new_dir_len = pow(2, GetGlobalDepth());
      std::vector<std::shared_ptr<Bucket>> temp_dir(new_dir_len);
      // 指针重分配
      size_t c = new_dir_len/2;
      for (size_t idx = 0; idx < c; idx++) {
        temp_dir[idx] = temp_dir[idx+c] = dir_[idx];
      }
      dir_.swap(temp_dir);
    }
    /**
     * 处理分裂出的新桶，也就是将对应的表项指针指向新桶，会出现两种情况
     * 第一种情况是桶深度为全局深度，此时需将高位置1，
     * 如下标为01的桶原先深度为两位，深度+1后下标变为001，分裂出的新桶下标应该为101
     * 第二种情况是桶深度小于全局深度，这种情况哈希后对应的表项索引值即是新桶的索引值
     */
      size_t nbucket_idx = 0;
      std::list<std::pair<K, V>> templist(dir_[dir_index]->GetItems());
      if (flag) nbucket_idx = dir_index + (1 << GetLocalDepth(dir_index));
      else
        nbucket_idx = dir_index;  // 如果本地深度小于全局深度，直接在该下标开辟新桶
      dir_[dir_index]->IncrementDepth();
      // 重新分配旧桶元素,清空
      dir_[dir_index]->ClearBucket();
      dir_[nbucket_idx].reset(new Bucket(bucket_size_, GetLocalDepth(dir_index)));
      // 重新进行插入操作，这里可以实现递归，因为可能出现分裂多次的情况
      for (typename::std::list<std::pair< K, V> >::iterator it = templist.begin(); it != templist.end(); it++) {
        Insert(it->first, it->second);
      }
      Insert(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::list<std::pair<K, V> > li = this->GetItems();
  for (typename::std::list<std::pair< K, V> >::iterator it = li.begin(); it != li.end(); it++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (typename::std::list<std::pair< K, V> >::iterator it = list_.begin(); it != list_.end(); ) {
    if (it->first == key) {
      list_.erase(it++);
      return true;
    } else {
      it++;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (this->IsFull()) return false;
  for (typename::std::list<std::pair< K, V> >::iterator it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  std::pair<K , V > ele(key, value);
  list_.push_back(ele);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
