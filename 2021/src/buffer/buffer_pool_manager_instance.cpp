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

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(static_cast<page_id_t>(instance_index)),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // 标记是脏页，将不活跃数据库页保存到下层绑定的文件页
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *page = pages_ + frame_id;
  page->is_dirty_ = false;
  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.begin();
  while (iter != page_table_.end()) {
    page_id_t page_id = iter->first;
    frame_id_t frame_id = iter->second;
    Page *page = pages_ + frame_id;
    disk_manager_->WritePage(page_id, page->GetData());
    iter++;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 1. 空闲链表不为空，从空闲链表获取页节点，将页结点的数据重置，返回。
  // 2. 空闲链表为空，lru取出牺牲节点
  // 3. 如果牺牲节点标记脏页，保存到下层
  // 4.
  // 将页哈希表对牺牲节点的页编号到页数组下标的映射，改为新页节点的页编号到页数组下标的映射，并将数据节点重置为新节点，返回。

  std::lock_guard<std::mutex> guard(latch_);

  // 从不活跃数据库页获取
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    page = &pages_[frame_id];
    // 牺牲页保存到下一层
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  }

  if (page != nullptr) {
    // 更新元数据
    *page_id = AllocatePage();
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page->ResetMemory();
    page_table_[*page_id] = frame_id;
    replacer_->Pin(frame_id);
    return page;
  }
  return nullptr;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1. 页哈希表判断页节点是否在页数组中
  //   a. 页节点在页数组中，页节点线程计数++，从lru删除，返回页节点
  // 2. 页节点不在页数组，说明在下层，需要取出来，需要从空闲链表找到一个空页节点。
  //   a. 判断空闲链表是否为空
  //   b. 空闲链表不为空，得到空页节点
  //   c. 空闲链表为空，lru取出牺牲节点
  //   d. 如果牺牲节点标记脏页，保存到下层，同样得到空页节点
  // 3. 从下层读取数据到空页节点，返回
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    // P存在
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }

  // 从不活跃区域获取
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    page = &pages_[frame_id];
    // 脏页
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    // 从哈希表删除
    page_table_.erase(page->GetPageId());
  }

  if (page != nullptr) {
    // P的元数据
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    // 磁盘
    disk_manager_->ReadPage(page_id, page->GetData());
    page_table_[page_id] = frame_id;
    replacer_->Pin(frame_id);
  }
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 1. 页哈希表判断页节点是否在页数组
  //   a. 页节点不在页数组，直接返回
  // 2. 页节点在页数组
  // 3. 如果页节点标记脏页，保存到下层
  // 4. 从lru删除节点
  // 5. 页节点清空数据
  // 6. 页节点放入空闲链表，实际上是放入页数组的下标
  std::lock_guard<std::mutex> guard(latch_);
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = iter->second;
  Page *page = pages_ + frame_id;
  if (page->pin_count_ > 0) {
    return false;
  }

  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }
  replacer_->Pin(frame_id);
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // 1. 页哈希表判断页节点是否在页数组
  //   a. 页节点不在页数组，直接返回
  // 2. 页节点在页数组
  // 3. 页节点引用计数--
  // 4. 如果参数说明脏页，则页节点标记脏页
  // 5. 如果页节点引用计数变为0，lru插入节点
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  // 找到了
  frame_id_t frame_id = iter->second;
  Page *page = pages_ + frame_id;
  if (page->GetPinCount() <= 0) {
    return false;
  }

  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  page->pin_count_--;
  if (page->GetPinCount() <= 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);
}

}  // namespace bustub
