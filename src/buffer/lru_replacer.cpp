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
/// FIXME(bayes): Valgrind bug. Don't call std::unordered_map::reserve which makes the valgrind timeout.
LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::mutex> lck{latch_};

  // if no frame existed.
  if (ump_.empty()) {
    return false;
  }

  // pop LRU out from list.
  assert(!lst_.empty());
  const frame_id_t key = lst_.back();
  lst_.pop_back();
  // remove it from hash map.
  ump_.erase(key);

  assert(lst_.size() == ump_.size());

  *frame_id = key;

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lck{latch_};

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
  std::scoped_lock<std::mutex> lck{latch_};

  // if the frame already exists.
  if (ump_.count(frame_id) == 1) {
    return;
  }

  // add the frame both in the hash map and the frame array.
  lst_.push_front(frame_id);
  ump_.insert({frame_id, lst_.begin()});
}

size_t LRUReplacer::Size() {
  std::scoped_lock<std::mutex> lck{latch_};
  return ump_.size();
}

}  // namespace bustub
