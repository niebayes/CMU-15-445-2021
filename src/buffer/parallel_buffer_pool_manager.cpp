//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// niebayes 2021-11-02
// niebayes@gmail.com

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_{num_instances}, pool_size_{pool_size}, round_robin_idx_{0} {
  // Allocate and create individual BufferPoolManagerInstances
  bpms_ = new BufferPoolManager*[num_instances_];
  assert(bpms_);
  for (int i = 0; i < num_instances_; ++i) {
    /// @bayes: static type: base class type; dynamic type: derived class type.
    bpms_[i] = new BPMInstance(pool_size_, num_instances_, i, disk_manager, log_manager);
    assert(bpms_[i]);
  }
}

// Update destructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  if (bpms_) {
    for (int i = 0; i < num_instances_; ++i) {
      delete bpms_[i];
      bpms_[i] = nullptr;
    }
    delete[] bpms_;
    bpms_ = nullptr;
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  /// FIXME(bayes): How should I properly handle this exception scenario?
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  // a simple mapping function: modulo operator.
  const size_t bpm_idx = page_id % num_instances_;
  return bpms_[bpm_idx];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto* bpm = GetBufferPoolManager(page_id);
  if (!bpm) {
    return nullptr;
  }
  /// FIXME(bayes): why can't I access base class protected methods using a pointer of type base class?
  return bpm->FetchPgImp(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto* bpm = GetBufferPoolManager(page_id);
  if (!bpm) {
    return false;
  }

  return bpm->UnpinPgImp(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto* bpm = GetBufferPoolManager(page_id);
  if (!bpm) {
    return false;
  }

  return bpm->FlushPgImp(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  
  Page* page{nullptr};
  // each searching only runs a loop, i.e. num_instances_ time. 
  // this is critical to ensure the system won't halt here too long.
  for (int i = 0; i < num_instances_; ++i) {
    if ((page = bpms_[round_robin_idx_++ % num_instances_]->NewPgImp(page_id))) {
      break;
    }
  }

  return page;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto* bpm = GetBufferPoolManager(page_id);
  if (!bpm) {
    return false;
  }

  return bpm->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (int i = 0; i < num_instances_; ++i) {
    auto* bpm = bpms_[i];
    assert(bpm);
    bpm->FlushAllPgsImp();
  }
}

}  // namespace bustub
