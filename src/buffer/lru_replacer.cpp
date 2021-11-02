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

#include <cassert>

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  // reserve space for hash map.
  ump_.reserve(num_pages);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // if no frame existed.
  if (ump_.empty()) {
    return false;
  }

  // evict the least recently used frame.
  *frame_id = lst_.back();
  lst_.pop_back();
  ump_.erase(*frame_id);
  assert(lst_.size() == ump_.size());

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // if the frame does not exist.
  if (ump_.count(frame_id) == 0) {
    return;
  }

  // remove the frame both from the hash map and the list.
  lst_.erase(ump_.at(frame_id));
  ump_.erase(frame_id);
  assert(lst_.size() == ump_.size());
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // add the newly unpinned both to the hash map and the list.
  lst_.push_front(frame_id);
  ump_[frame_id] = lst_.begin();
  assert(lst_.size() == ump_.size());
}

size_t LRUReplacer::Size() { return ump_.size(); }

}  // namespace bustub
