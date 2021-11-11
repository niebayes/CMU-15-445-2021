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

// niebayes 2021-11-02
// niebayes@gmail.com

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

  std::scoped_lock<std::mutex> lck{latch_};

  Page *page{nullptr};
  frame_id_t frame_id = -1;

  if (page_table_.count(page_id) == 1) {
    frame_id = page_table_.at(page_id);
  }
  // if the page is not in the buffer pool, no-op.
  if (frame_id == -1) {
    return false;
  }

  assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  page = &pages_[frame_id];
  assert(page);

  // flush the data to disk no matter the page is dirty or not.
  disk_manager_->WritePage(page_id, page->GetData());
  // and the page is definitely not dirty after flushing.
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock<std::mutex> lck{latch_};

  for (const auto &[page_id, frame_id] : page_table_) {
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
    Page *page = &pages_[frame_id];
    assert(page);

    // flush the data to disk no matter the page is dirty or not.
    disk_manager_->WritePage(page_id, page->GetData());
    // and the page is definitely not dirty after flushing.
    page->is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  /// @bayes: what does this method do?
  /// It's used to buffer a new physical page.
  /// Before flushed to disk, the newly created data are stored temporarily in the buffer pool.
  /// NewPgImp looks for an unpinned frame and use it as the container to manage the data.

  std::scoped_lock<std::mutex> lck{latch_};

  // try to find an unpinned frame to be used as the data container.
  Page *page{nullptr};
  frame_id_t frame_id = -1;

  // first, lookup the free list.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // alternatively, try to evict one.
    if (!replacer_->Victim(&frame_id)) {
      // failed to evict. No unpinned frame was found.
      return nullptr;
    }
  }

  // found an unpinned frame!
  assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  page = &pages_[frame_id];
  assert(page);
  assert(page->GetPinCount() == 0);

  // allocate a new page id for the newly created physical page.
  const page_id_t new_page_id = AllocatePage();
  assert(new_page_id != INVALID_PAGE_ID);

  // flush the old page if it's from the replacer and it's dirty.
  const page_id_t old_page_id = page->GetPageId();
  if (old_page_id != INVALID_PAGE_ID && page->IsDirty()) {
    disk_manager_->WritePage(old_page_id, page->GetData());
    page->is_dirty_ = false;
  }

  // set the frame's metadata to make it track the new physical page.
  page->page_id_ = new_page_id;
  // ensure the frame is not in the replacer.
  replacer_->Pin(frame_id);
  // pin the page on this frame.
  page->pin_count_ = 1;
  assert(page->GetPinCount() == 1);

  // erase the old mapping and insert the new mapping in the page table.
  page_table_.erase(old_page_id);  // erase is not if the old_page_id does not exist.
  page_table_.insert({new_page_id, frame_id});

  // flush the new page immediately to make it persistent. The page id matters, not the data.
  page->ResetMemory();
  disk_manager_->WritePage(new_page_id, page->GetData());

  // set the output param.
  *page_id = new_page_id;

  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock<std::mutex> lck{latch_};

  Page *page{nullptr};
  frame_id_t frame_id = -1;

  if (page_table_.count(page_id) == 1) {
    frame_id = page_table_.at(page_id);
  }

  // if P exists, fetch the in-memory page directly.
  if (frame_id != -1) {
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
    page = &pages_[frame_id];
    assert(page);

    // ensure the frame won't be evicted.
    replacer_->Pin(frame_id);
    // pin the page on this frame.
    ++page->pin_count_;
    assert(page->GetPinCount() >= 1);

    return page;
  }

  // otherwise, P does not exist.
  // try to find an unpinned frame in which the fetched on-disk page is stored.

  // request a replacement frame from free list if it's not empty.
  //! search the free list first to minimize disk I/Os and hence better efficiency.
  if (!free_list_.empty()) {
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
  assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  page = &pages_[frame_id];
  assert(page);
  assert(page->GetPinCount() == 0);

  // flush the old page if it's from the replacer and it's dirty.
  const page_id_t old_page_id = page->GetPageId();
  if (old_page_id != INVALID_PAGE_ID && page->IsDirty()) {
    disk_manager_->WritePage(old_page_id, page->GetData());
    page->is_dirty_ = false;
  }

  // delete the old mapping and insert the new mapping.
  page_table_.erase(old_page_id);  // erase is no-op if the old_page_id does not exist.
  page_table_.insert({page_id, frame_id});

  // zero out old data.
  page->ResetMemory();

  // update the frame's metadata.
  page->page_id_ = page_id;
  // ensure this frame it's not in the replacer.
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;

  // read data in from disk.
  //! It's the caller's job to ensure that the page_id is valid, i.e. it corresponds to a physical page.
  disk_manager_->ReadPage(page_id, page->GetData());

  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::scoped_lock<std::mutex> lck{latch_};

  // no-op.
  DeallocatePage(page_id);

  // search the page table.
  frame_id_t frame_id = -1;
  if (page_table_.count(page_id) == 1) {
    frame_id = page_table_.at(page_id);
  }
  // P does not exist.
  if (frame_id == -1) {
    return true;
  }

  // get the page.
  assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  Page *page = &pages_[frame_id];
  assert(page);

  // check if we can thread-safely delete it.
  assert(page->GetPinCount() >= 0);
  if (page->GetPinCount() > 0) {
    // no, someone else is using it.
    return false;
  }
  assert(page->GetPinCount() == 0);

  // flush the page if it's dirty.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
  assert(!page->IsDirty());

  // zero out old data.
  page->ResetMemory();

  // reset metadata.
  page->page_id_ = INVALID_PAGE_ID;
  // ensure that the frame is not in the replacer.
  replacer_->Pin(frame_id);
  assert(page->GetPinCount() == 0);

  // remove the page from page table.
  page_table_.erase(page_id);

  // return it to free list.
  free_list_.push_front(frame_id);

  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lck{latch_};

  Page *page{nullptr};
  frame_id_t frame_id = -1;

  // check if the page is in the buffer pool.
  if (page_table_.count(page_id) == 1) {
    frame_id = page_table_.at(page_id);
  }
  if (frame_id == -1) {
    // does not exist.
    return false;
  }

  assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  page = &pages_[frame_id];
  assert(page);

  // decrement the pin count if it was pinned already.
  bool was_pinned{false};
  if (page->GetPinCount() > 0) {
    was_pinned = true;
    // if this unpinning decrements the pin count to zero, no threads are using it.
    // so send it to replacer.
    if (--page->pin_count_ == 0) {
      assert(page->GetPinCount() == 0);
      replacer_->Unpin(frame_id);
    }
  }
  // set the dirty flag.
  //! if it's already dirty, don't reset it!
  page->is_dirty_ |= is_dirty;

  return was_pinned;
}

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
