//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// niebayes 2021-11-02 
// niebayes@gmail.com

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!

  inline void Step(size_t *ptr) { *ptr = (*ptr + 1) % cap_; }

  // replacer capacity.
  const size_t cap_;
  size_t victim_ptr_;  // find victim frame starts here.
  size_t insert_ptr_;  // the next position to insert newly unpinned frames.

  // array of frames (ids).
  std::vector<frame_id_t> frames_;
  // hash map for efficiently looking for frames.
  // <frame_id, frame_idx_in_array>
  std::unordered_map<frame_id_t, size_t> ump_;
};

}  // namespace bustub
