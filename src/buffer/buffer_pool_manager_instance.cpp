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
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
  /**
   * 该功能是将页放如页框中
   * 1.如果空闲表没有位置，替换策略中发现所有页框都上了锁，表示失败直接返回空指针
   * 2.空闲表是否有位置，没有才用替换策略替换旧页；调用分配页功能来获取新页id。
   * 3.旧页刷脏，setevictalbe为false将该页pin住，同时将新页的数据重置（这里没有放新数据我不太理解），
   *   哈希表映射页->页框
   */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  if (free_list_.empty() && !replacer_->Size()) return nullptr;

  frame_id_t frame_id;
  page_id_t new_page_id = AllocatePage();

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else { 
    replacer_->Evict(&frame_id);
    if (pages_[frame_id].is_dirty_)
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }
  // 这里其实即使是同一个页框，也需要重新进行记录，因为该页框放的已经不是同一份数据（页）
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  Page new_page;
  new_page.page_id_ = new_page_id;
  pages_[frame_id] = new_page;
  page_table_->Insert(new_page_id, frame_id);
  *page_id = new_page_id;
  return &pages_[frame_id];
}
/**
  * 1.从缓冲区中查询是否有该页(哈希表查找)有直接返回;如果没有，则需要从从磁盘调页，用readpage函数来获取
  * 2.如果缓冲区中的页框都被使用和锁上，此时返回空指针表示失败
  * 3.放页策略依旧时首先找是否存在空页框，如果没有再用replacer
  * 4.刷脏，pin住该页，哈希表映射页->页框
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }
  else if (free_list_.empty() && !replacer_->Size()) {
    return nullptr;
  }
  Page disk_page;
  disk_page.page_id_ = page_id;
  disk_manager_->ReadPage(page_id,disk_page.data_);
  if (!free_list_.empty())
  {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    if (pages_[frame_id].is_dirty_)
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id] = disk_page;
  page_table_->Insert(frame_id,page_id);
  return &pages_[frame_id];

}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { return false; }

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { return false; }

void BufferPoolManagerInstance::FlushAllPgsImp() {}
  /**
   * 删除指定页面，如果缓冲池中没有该页则直接返回true；如果被pin住则返回false。
   * 删除页面后在替换策略中停止追踪该页，同事将空下来的页框放到空闲列表中，释放页
   * 面内存和元数据，最后在磁盘上释放该页，需要调用DeallocatePage() ，但目前没
   * 有实现，所以仅是做做样子
   */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return next_page_id_++; }

}  // namespace bustub
