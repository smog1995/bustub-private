//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <iostream>
#include <shared_mutex>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
/**
 * 该功能是将页放如页框中
 * 1.如果空闲表没有位置，替换策略中发现所有页框都上了锁，表示失败直接返回空指针
 * 2.空闲表是否有位置，没有才用替换策略替换旧页；调用分配页功能来获取新页id,；
 * 3.使用替换策略时需要看脏位来考虑旧页写回磁盘，set_evictalbe为false将该页pin住，哈希表映射页->页框
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "NewPgImp:";
  if (free_list_.empty() && replacer_->Size() == 0) {
    // std::cout << "freelist is full and all frames are pinned." << std::endl;
    return nullptr;
  }

  frame_id_t frame_id;
  page_id_t new_page_id = AllocatePage();
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    page_table_->Remove(pages_[frame_id].page_id_);
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
  }
  // 这里其实即使是同一个页框，也需要重新进行记录，因为该页框放的已经不是同一份数据（页）
  replacer_->RecordAccess(frame_id);
  pages_[frame_id].pin_count_++;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = new_page_id;
  page_table_->Insert(new_page_id, frame_id);
  *page_id = new_page_id;
  return pages_ + frame_id;
}
/**
 * 1.从缓冲区中查询是否有该页(哈希表查找)有直接返回;如果没有，则需要从从磁盘调页，用readpage函数来获取
 * 2.如果缓冲区中的页框都被使用和锁上，此时返回空指针表示失败
 * 3.放页策略依旧时首先找是否存在空页框，如果没有再用replacer
 * 4.刷脏，pin住该页，哈希表映射页->页框
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "fetchpgImp:" << page_id;
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    // std::cout << " it exists in buffer pool ,return it." << std::endl;
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    // 重新被pin住，需要设置为不可驱逐
    if (pages_[frame_id].pin_count_ == 1) {
      replacer_->SetEvictable(frame_id, false);
    }
    return pages_ + frame_id;
  }
  if (free_list_.empty() && replacer_->Size() == 0) {
    // std::cout << " it doesn't exists in buffer pool, and freelist is full and all frames are pinned." << std::endl;
    return nullptr;
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    page_table_->Remove(pages_[frame_id].page_id_);
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
  }
  // std::cout << " it doesn't exists in buffer pool,new frame_id is:" << frame_id << std::endl;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  replacer_->RecordAccess(frame_id);
  pages_[frame_id].pin_count_++;
  page_table_->Insert(page_id, frame_id);
  return pages_ + frame_id;
}

/**
 * 如果pin_count大于0，pin_count_--，若-1后等于0，需要将其设置为可驱逐状态。
 */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id) && pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].pin_count_--;
    if (!pages_[frame_id].is_dirty_) {
      pages_[frame_id].is_dirty_ = is_dirty;
    }
    if (pages_[frame_id].pin_count_ <= 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    // std::cout << "unpinpgimp success:" << page_id << " is_dirty:" << is_dirty << std::endl;
    return true;
  }
  // std::cout << "unpinpgimp failed:" << page_id << std::endl;
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "flushPgImp:" << page_id;
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // std::cout << "this page doesnt exists in buffer pool." << std::endl;
    return false;
  }
  disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  // std::cout << "write to disk success." << std::endl;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "flushallpgsimp" << std::endl;
  for (size_t index = 0; index < pool_size_; index++) {
    if (pages_[index].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[index].page_id_, pages_[index].data_);
      pages_[index].is_dirty_ = false;
    }
  }
}
/**
 * 删除指定页面，如果缓冲池中没有该页则直接返回true；如果被pin住则返回false。
 * 删除页面后在替换策略中停止追踪该页，同事将空下来的页框放到空闲列表中，释放页
 * 面内存和元数据，最后在磁盘上释放该页，需要调用DeallocatePage() ，但目前没
 * 有实现，所以仅是做做样子
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // std::cout << "deletePgImp:" << page_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // std::cout << " it doesnt exists in buffer pool." << std::endl;
    return true;
  }
  if (pages_[frame_id].pin_count_ <= 0) {
    // std::cout << " result: delete success!" << std::endl;
    free_list_.push_back(frame_id);
    replacer_->Remove(frame_id);
    pages_[frame_id].ResetMemory();
    DeallocatePage(page_id);
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
