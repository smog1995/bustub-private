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
  size_t dir_index=IndexOf(key);
  return dir_[dir_index]->Find(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t dir_index=IndexOf(key);

  return dir_[dir_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //桶的个数为0，需要创建
  if(!this->GetGlobalDepthInternal())
  {
    std::share_ptr<Bucket> p(new Bucket(this->bucket_size,1));
    dir_.push_back(p);
    this->IncrementGlobalDepth();
  }
  size_t dir_index=IndexOf(key);
  if(!dir_[idx_index]->Insert(key,value))//进行插入操作，当插入失败时
  {
    if(this->GetLocalDepthInternal(dir_index)==this->GetGlobalDepth())
    {
      //首先让哈希表长度乘以2
      this->IncrementGlobalDepth();
      for(int i=0;i<num_buckets_;i++)
      {
        std::share_ptr<Bucket> p(new Bucket(this->bucket_size,1));
        dir_.push_back();
      }
      num_buckets_*=2;

    }
  }
  
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::list<std::pair<K, V> > li=GetItems();
  for(std::list<std::pair< K, V> >::iterator it=li;it!=li.end();it++)
  {
    if(it->first==key) {
      value=it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  std::list<std::pair<K, V>> li=GetItems();
  for(std::list<std::pair< K, V> >::iterator it=li;it!=li.end();)
  {
    if(it->first==key)
    {
      li.erase(it++);
      return true;
    }
    else it++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if(this->IsFull()) return false;
  std::scoped_lock<std::mutex> lock(latch_);
  std::list<std::pair<K, V>> li=this->GetItems();
  for(std::list<std::pair< K, V> >::iterator it=li;it!=li.end();it++)
  {
    if(it->first==key)
    {
      it->second=value;
      return true;
    }
  }
  std::pair<K,V> ele=make_pair(key,value);
  li.push_back(ele);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
