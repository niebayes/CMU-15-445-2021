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
      next_page_id_(instance_index),
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
  // Make sure you call DiskManager::WritePage!

  Page* page{nullptr};
  frame_id_t frame_id = -1;

  latch_.lock();
  if (page_table_.count(page_id)) {
    frame_id = page_table_.at(page_id);
  }
  latch_.unlock();
  if (frame_id == -1) {
    return false;
  }

  assert(frame_id >= 0 && frame_id < pool_size_);
  page = &pages_[frame_id];
  assert(page);

  // flush the data to disk.
  // dirty flag does not involve with flushing.
  disk_manager_->WritePage(page_id, page->GetData());

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!

  /// TODO(bayes): protect the concurrent accessing of page table.

  /// TODO(bayes): replace this with structure binding.
  for (const auto& p : page_table_) {
    FlushPgImp(p.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return nullptr;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  Page* page{nullptr};

  frame_id_t frame_id = -1;
  latch_.lock();
  if (page_table_.count(page_id)) {
    frame_id = page_table_.at(page_id);
  }
  latch_.unlock();

  // if P exisis, fetch the in-memory page directly.
  if (frame_id != -1) {
    assert(frame_id >= 0 && frame_id < pool_size_);
    page = &pages_[frame_id];
    assert(page);

    // pin this page.
    page->WLatch();
    ++page->pin_count_;
    page->WUnlatch();

    return page;
  }

  // otherwise, P does not exist. 
  // try to find an unpinned frame in which the fetched on-disk page is stored.

  // request a replacement frame from free list if it's not empty.
  if (free_list_.size() > 0) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // otherwise, try to evict one.
    if (!replacer_->Victim(&frame_id)) {
      // failed to evict.
      return nullptr;
    }
  }

  // found an unpinned frame!
  assert(frame_id >= 0 && frame_id < pool_size_);
  page = &pages_[frame_id];
  assert(page);
  assert(page->GetPinCount() == 0);

  // if it's dirty, flush it to disk.
  page_id_t old_page_id;
  page->WLatch();
  old_page_id = page->GetPageId();
  if (page->IsDirty()) {
    const bool success = FlushPgImp(old_page_id);
    assert(success);
    page->is_dirty_ = false;
  }
  page->WUnlatch();

  // delete the page's corresponding entry from page table.
  latch_.lock();
  page_table_.erase(old_page_id);
  // and insert the new mapping from the fetched physical page to the replacement frame.
  page_table_.insert({page_id, frame_id});
  latch_.unlock();

  // update the frame's metadata and read data in from disk.
  page->WLatch();
  page->page_id_ = page_id;
  // pin the page on this frame.
  page->pin_count_ = 1;
  page->ResetMemory();
  /// FIXME(bayes): Does the caller assure that the page exists on disk?
  disk_manager_->ReadPage(page_id, page->GetData());
  page->WUnlatch();

  return nullptr;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  // no-op.
  DeallocatePage(page_id);

  // search the page table.
  frame_id_t frame_id = -1;
  latch_.lock();
  if (page_table_.count(page_id)) {
    frame_id = page_table_.at(page_id);
  } 
  latch_.unlock();
  // P does not exist.
  if (frame_id == -1) {
    return true;
  }

  // get the page.
  assert(frame_id >= 0 && frame_id < pool_size_);
  Page* page = &pages_[frame_id];
  assert(page);

  // read its pin count.
  int pin_cnt;
  page->RLatch();
  pin_cnt = page->GetPinCount();
  page->RUnlatch();

  // check if we can thread-safely delete it.
  assert(pin_cnt >= 0);
  if (pin_cnt > 0) {
    // no, someone else is using it.
    return false;
  }

  // remove the page from page table.
  latch_.lock();
  page_table_.erase(page_id);
  latch_.unlock();

  // reset the page's data and metadata.
  page->WLatch();
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->WUnlatch();

  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { return false; }

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
