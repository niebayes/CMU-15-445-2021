//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// niebayes 2021-11-02 
// niebayes@gmail.com

#include <cassert>

#include "buffer/lru_replacer.h"

namespace bustub {

/// @bayes: the order of init members is critical!
LRUReplacer::LRUReplacer(size_t num_pages) : cap_{num_pages}, victim_ptr_{0}, insert_ptr_{0} {
  frames_.resize(cap_, INVALID_PAGE_ID);
  ump_.reserve(cap_);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // if no frame existed.
  if (ump_.empty()) {
    return false;
  }

  // search for a victim page.
  while (frames_.at(victim_ptr_) == INVALID_PAGE_ID) {
    Step(&victim_ptr_);
  }

  // evict it and remove it from the hash map.
  *frame_id = frames_.at(victim_ptr_);
  ump_.erase(frames_.at(victim_ptr_));
  frames_.at(victim_ptr_) = INVALID_PAGE_ID;
  Step(&victim_ptr_);

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // if the frame does not exist.
  if (ump_.count(frame_id) == 0) {
    return;
  }

  // remove the frame both from the hash map and the frame array.
  frames_.at(ump_.at(frame_id)) = INVALID_PAGE_ID;
  ump_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // if the frame already exists.
  if (ump_.count(frame_id) == 1) {
    return;
  }

  // add the frame both in the hash map and the frame array.
  frames_.at(insert_ptr_) = frame_id;
  ump_[frame_id] = insert_ptr_;
  Step(&insert_ptr_);
}

size_t LRUReplacer::Size() { return ump_.size(); }

}  // namespace bustub
