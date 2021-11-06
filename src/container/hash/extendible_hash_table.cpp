//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  assert(buffer_pool_manager_ != nullptr);

  // request a new page to be used as the directory page.
  auto *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  dir_page->SetPageId(directory_page_id_);  //! not necessary, but some tests check this.

  // request a new page to be used as the first bucket page.
  page_id_t bucket_page_id{INVALID_PAGE_ID};
  assert(buffer_pool_manager_->NewPage(&bucket_page_id) != nullptr);

  // link the first directory entry with the bucket page.
  dir_page->SetLocalDepth(0, 0);
  dir_page->SetBucketPageId(0, bucket_page_id);

  //! debug.
  dir_page->VerifyIntegrity();

  // unpin pages. Marked as dirty to make them persistent.
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return (Hash(key) & dir_page->GetGlobalDepthMask());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  bool found_key{false};

  table_latch_.RLock();

  auto *dir_page = FetchDirectoryPage();
  const page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  found_key = bucket_page->GetValue(key, comparator_, result);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));

  table_latch_.RUnlock();

  return found_key;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  auto *dir_page = FetchDirectoryPage();
  const page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);
  assert(bucket_page != nullptr);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  const bool is_full = bucket_page->IsFull();
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  /// FIXME(bayes): what if another thread inserts a key into the bucket at this time and makes it full?

  // if not full, a trivial insertion.
  if (!is_full) {
    reinterpret_cast<Page *>(bucket_page)->WLatch();
    const bool dirty_flag = bucket_page->Insert(key, value, comparator_);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();

    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, dirty_flag));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    table_latch_.RUnlock();

    return dirty_flag;
  }

  // otherwise, has to do bucket splitting and potential directory expansion.

  /// FIXME(bayes): Is it safe to switch lock modes in this way? We need lock promotion!!!
  table_latch_.RUnlock();
  table_latch_.WLock();

  // expand directory if necessary.
  const uint32_t dir_idx = KeyToDirectoryIndex(key, dir_page);
  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(dir_idx)) {
    const uint32_t cur_size = dir_page->Size();
    for (uint32_t i = 0; i < cur_size; ++i) {
      dir_page->SetBucketPageId(i + cur_size, dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(i + cur_size, dir_page->GetLocalDepth(i));
    }
    // double size.
    dir_page->IncrGlobalDepth();

    //! debug.
    dir_page->VerifyIntegrity();
  }

  // collect all linked directory entries with the overflowing bucket page.
  std::vector<uint32_t> linked_entries;
  linked_entries.reserve(dir_page->Size() / 2);  // at most half.
  for (uint32_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      linked_entries.push_back(i);
      // increment their local depths BTW.
      dir_page->IncrLocalDepth(i);
    }
  }
  assert(!linked_entries.empty());

  //! debug.
  dir_page->VerifyIntegrity();

  // request a new page to be used as the split image.
  page_id_t split_img_id;
  assert(buffer_pool_manager_->NewPage(&split_img_id) != nullptr);
  auto *split_img = FetchBucketPage(split_img_id);

  // relink according to the mask result between the directory index and the high bit.
  // those with high bit 0 are linked with the overflowing bucket page.
  // those with high bit 1 are linked with the split image.
  const uint32_t high_bit = dir_page->GetLocalHighBit(dir_idx);
  for (const uint32_t &i : linked_entries) {
    if ((i & high_bit) != 0) {
      dir_page->SetBucketPageId(i, split_img_id);
    }
  }
  //! debug.
  dir_page->VerifyIntegrity();

  // reinsert the key-value pairs in the overflowing bucket page. Also by inspecting the high bit.
  // those with high bit 0 stay in the overflowing bucket page.
  // those with high bit 1 are removed from the overflowing bucket page and reinserted to the split image.
  /// FIXME(bayes): Is it necessary to lock split_img as well?
  bool move_occur{false};  // Did data moving happen?
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  reinterpret_cast<Page *>(split_img)->WLatch();
  assert(bucket_page->IsFull());  // hence NumReadble = BUCKET_ARRAY_SIZE.
  for (uint32_t i = 0; i < bucket_page->NumReadable(); ++i) {
    const uint32_t key_hash = Hash(bucket_page->KeyAt(i));
    if ((key_hash & high_bit) != 0) {
      assert(split_img->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), comparator_));
      bucket_page->RemoveAt(i);
      move_occur |= true;
    }
  }
  reinterpret_cast<Page *>(split_img)->WUnlatch();
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  // unpin pages.
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, move_occur));
  assert(buffer_pool_manager_->UnpinPage(split_img_id, true));  // marked as dirty to be persistent.

  table_latch_.WUnlock();

  // retry insertion.
  return SplitInsert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  auto *dir_page = FetchDirectoryPage();
  const page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  // check if the key-value was found and removed.
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  const bool removed = bucket_page->Remove(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  if (!removed) {
    // the key-value pair was not found.

    // unpin pages.
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    table_latch_.RUnlock();
    return false;
  }
  // the key-value pair was removed.

  // check if this removal makes the bucket page empty.
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  const bool empty = bucket_page->IsEmpty();
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  if (!empty) {
    // not empty. No need to merge buckets.

    // unpin pages.
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    table_latch_.RUnlock();
    return true;
  }

  // has to do bucket merging.
  table_latch_.RUnlock();
  table_latch_.WLock();
  Merge(transaction, key, value);

  // unpin pages.
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);  // this page may be removed due to directory shrinking.
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));

  table_latch_.WUnlock();

  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
