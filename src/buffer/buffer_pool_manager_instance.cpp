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
   * 该功能是将页放如页框中，首先看空闲表是否有位置，没有才用替换策略替换旧页；调用分配页功能来获取新页id。
   * 替换旧页需要将其写回磁盘，将新页放入时，需要用setevictalbe为false来钉住该页
   */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  if (free_list_.empty() && !replacer_->Size()) return nullptr;

  frame_id_t frame_id;
  page_id_t page_id==AllocatePage();;
  Page* pages=GetPages();
  // 如果空闲表中还有位置
  if (!free_list_.empty()) {
    frame_id=free_list_.front();
    free_list_.pop_front();
    replacer_->RecordAccess(frame_id);
  }
  pages[frame_id].SetPageId(page_id);
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { return nullptr; }

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
