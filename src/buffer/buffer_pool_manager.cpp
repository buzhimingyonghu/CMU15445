//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {  // 锁定以保护对缓冲池的访问
  auto guard = std::lock_guard(latch_);

  // 分配一个空闲的帧
  frame_id_t frame_id = AllocateFrame();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;  // 如果没有空闲帧，返回 nullptr
  }

  // 分配一个新页面 ID
  *page_id = AllocatePage();
  pages_[frame_id].ResetMemory();        // 重置页面内存
  pages_[frame_id].page_id_ = *page_id;  // 设置页面 ID
  pages_[frame_id].pin_count_ = 1;       // 将页面固定
  pages_[frame_id].is_dirty_ = false;    // 页面初始状态为干净

  replacer_->RecordAccess(frame_id);         // 记录对页面的访问
  replacer_->SetEvictable(frame_id, false);  // 设置页面为不可逐换
  page_table_[*page_id] = frame_id;          // 更新页表

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto guard = std::lock_guard(latch_);

  // 检查页面是否已存在于缓冲池中
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    auto frame_id = iter->second;

    // 页面已存在，增加固定计数
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);  // 记录访问
    replacer_->SetEvictable(frame_id, false);        // 设置为不可逐换

    return &pages_[frame_id];
  }

  // 页面不在缓冲池中，分配一个空闲的帧
  frame_id_t frame_id = AllocateFrame();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;  // 如果没有空闲帧，返回 nullptr
  }

  // 从磁盘读取页面到缓冲池中
  ReadFrame(frame_id, page_id);
  pages_[frame_id].page_id_ = page_id;  // 设置页面 ID
  pages_[frame_id].pin_count_ = 1;      // 将页面固定
  pages_[frame_id].is_dirty_ = false;   // 页面初始状态为干净

  replacer_->RecordAccess(frame_id, access_type);  // 记录访问
  replacer_->SetEvictable(frame_id, false);        // 设置为不可逐换
  page_table_[page_id] = frame_id;                 // 更新页表

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto guard = std::lock_guard(latch_);

  // 检查页面是否在缓冲池中
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;  // 页面不存在
  }

  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;  // 页面固定计数无效
  }

  // 解固定页面，更新脏状态
  --pages_[frame_id].pin_count_;
  pages_[frame_id].is_dirty_ |= is_dirty;

  // 如果页面不再被固定，设置为可逐换
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto guard = std::lock_guard(latch_);

  BUSTUB_ENSURE(page_id != INVALID_PAGE_ID, "Page id is ensured to be valid.");  // 确保页面 ID 有效

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;  // 页面不存在
  }

  auto frame_id = page_table_[page_id];
  WriteFrame(frame_id, page_id);  // 写回页面到磁盘

  pages_[frame_id].is_dirty_ = false;  // 标记页面为干净
  return true;
}

void BufferPoolManager::FlushAllPages() {
  auto guard = std::lock_guard(latch_);

  for (const auto &[page_id, frame_id] : page_table_) {
    WriteFrame(frame_id, page_id);       // 写回页面到磁盘
    pages_[frame_id].is_dirty_ = false;  // 标记页面为干净
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto guard = std::lock_guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;  // 页面不存在
  }

  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;  // 页面仍被固定，不能删除
  }

  // 从页表中移除页面，加入自由列表
  page_table_.erase(page_id);
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();               // 重置页面内存
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;  // 设置页面 ID 为无效
  pages_[frame_id].pin_count_ = 0;              // 固定计数设为 0
  pages_[frame_id].is_dirty_ = false;           // 页面初始状态为干净

  DeallocatePage(page_id);  // 释放页面 ID
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();  // 申请读锁
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }
auto BufferPoolManager::AllocateFrame() -> frame_id_t {
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      WriteFrame(frame_id, pages_[frame_id].GetPageId());
    }

    page_table_.erase(pages_[frame_id].GetPageId());
  }

  return frame_id;
}
void BufferPoolManager::ReadFrame(frame_id_t frame_id, page_id_t page_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  auto request = DiskRequest{
      .is_write_ = false,
      .data_ = pages_[frame_id].GetData(),
      .page_id_ = page_id,
      .callback_ = std::move(promise),
  };

  disk_scheduler_->Schedule(std::move(request));
  future.get();
}

void BufferPoolManager::WriteFrame(frame_id_t frame_id, page_id_t page_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  auto request = DiskRequest{
      .is_write_ = true,
      .data_ = pages_[frame_id].GetData(),
      .page_id_ = page_id,
      .callback_ = std::move(promise),
  };

  disk_scheduler_->Schedule(std::move(request));
  future.get();
}
}  // namespace bustub
