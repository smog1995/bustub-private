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
#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <list>
#include <utility>
#include "storage/page/page.h"

namespace bustub {

auto Pow(size_t a, size_t b) -> size_t {
  size_t ans = 1;

  for (size_t i = 0; i < b; i++) {
    ans *= a;
  }

  return ans;
}

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
  size_t depth;
  std::scoped_lock<std::mutex> lock(latch_);
  if (!dir_.empty() && (dir_index >= 0 && dir_index < static_cast<int>(Pow(2, global_depth_)))) {
    depth = GetLocalDepthInternal(dir_index);
  } else {
    depth = 0;
  }
  // PrintAll();
  return depth;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  // PrintAll();
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool result;
  if (dir_.empty()) {
    result = false;
  } else {
    size_t dir_index = IndexOf(key);
    result = dir_[dir_index]->Find(key, value);
  }
  // PrintAll();
  return result;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool result;
  if (dir_.empty()) {
    result = false;
  } else {
    size_t dir_index = IndexOf(key);
    result = dir_[dir_index]->Remove(key);
  }
  return result;
}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (dir_.empty()) {
    std::shared_ptr<Bucket> p(new Bucket(bucket_size_, 0));
    dir_.push_back(p);
  }
  InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  // std::cout << "insert" << key << std::endl;
  size_t dir_index = IndexOf(key);
  if (!dir_[dir_index]->Insert(key, value)) {
    bool flag = false;
    if (dir_[dir_index]->GetDepth() == global_depth_) {
      flag = true;
      global_depth_++;
      size_t new_dir_len = Pow(2, global_depth_);
      std::vector<std::shared_ptr<Bucket>> temp_dir(new_dir_len);
      size_t c = new_dir_len / 2;
      for (size_t idx = 0; idx < c; idx++) {
        temp_dir[idx] = temp_dir[idx + c] = dir_[idx];
      }
      dir_.swap(temp_dir);
    }
    dir_[dir_index]->IncrementDepth();
    size_t new_bucket_index;
    std::list<std::pair<K, V>> templist(dir_[dir_index]->GetItems());
    if (flag) {
      new_bucket_index = dir_index + (1 << (dir_[dir_index]->GetDepth() - 1));
      dir_[new_bucket_index].reset(new Bucket(bucket_size_, dir_[dir_index]->GetDepth()));
    } else {
      int new_bucket_highest_bit = 1 << (dir_[dir_index]->GetDepth() - 1);
      dir_index = dir_index & (new_bucket_highest_bit - 1);
      new_bucket_index = dir_index + new_bucket_highest_bit;
      dir_[new_bucket_index].reset(new Bucket(bucket_size_, dir_[dir_index]->GetDepth()));
      size_t count = Pow(2, global_depth_);
      size_t mask = (new_bucket_highest_bit << 1) - 1;

      for (size_t index = 0; index < count; index++) {
        if ((index & mask) == dir_index) {
          dir_[index] = dir_[dir_index];
        } else if ((index & mask) == new_bucket_index) {
          dir_[index] = dir_[new_bucket_index];
        }
      }
    }
    num_buckets_++;
    dir_[dir_index]->ClearBucket();
    for (auto it = templist.begin(); it != templist.end(); it++) {
      InsertInternal(it->first, it->second);
    }
    InsertInternal(key, value);
  }
}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::PrintAll() const {
  if (dir_.empty()) {
    return;
  }
  int num = Pow(2, global_depth_);
  for (int index = 0; index < num; index++) {
    std::list<std::pair<K, V>> li = dir_[index]->GetItems();
    std::cout << "bucket " << index << ":";
    // LOG_INFO("bucket %d:",index);
    for (auto &ele : li) {
      std::cout << " <<" << ele.first << ">> ";
    }
    // std::cout << "该桶的深度：" << dir_[index]->GetDepth() << std::endl;
  }

  // std::cout << "全局深度：" << global_depth_ << "桶数量" << num_buckets_ << std::endl;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = list_.begin();
  bool result = false;
  while (it != list_.end()) {
    if (it->first != key) {
      it++;
    } else {
      value = it->second;
      result = true;
      break;
    }
  }
  return result;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  bool result = false;
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first != key) {
      it++;
    } else {
      list_.erase(it);
      result = true;
      break;
    }
  }
  return result;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  bool result = false;
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first != key) {
      it++;
    } else {
      it->second = value;
      result = true;
      break;
    }
  }
  if (!result && !IsFull()) {
    std::pair<K, V> ele(key, value);
    list_.push_back(ele);
    return true;
  }
  return result;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
