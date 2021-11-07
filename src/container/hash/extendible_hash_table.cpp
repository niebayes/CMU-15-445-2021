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

// toggle the kth bit.
#define BIT_TOGGLE(x, k) ((x) ^= (1U << (k)))

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  assert(buffer_pool_manager_ != nullptr);

  /// FIXME(bayes): Is it legal to hold lock in the ctor?
  table_latch_.WLock();

  /// FIXME(bayes): also risky on no spare frames.
  // request a new page to be used as the directory page.
  auto *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  assert(dir_page != nullptr);
  dir_page->SetPageId(directory_page_id_);  //! not necessary, but some tests check this.

  // request a new page to be used as the first bucket page.
  page_id_t bucket_page_id{INVALID_PAGE_ID};
  // assert(buffer_pool_manager_->NewPage(&bucket_page_id) != nullptr);
  auto *bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&bucket_page_id)->GetData());
  assert(bucket_page != nullptr);

  // link the first directory entry with the bucket page.
  dir_page->SetLocalDepth(0, 0);
  dir_page->SetBucketPageId(0, bucket_page_id);

  // //! debug.
  // dir_page->VerifyIntegrity();

  // flush the pages to make them persistent.
  assert(buffer_pool_manager_->FlushPage(bucket_page_id));
  assert(buffer_pool_manager_->FlushPage(directory_page_id_));

  // unpin pages.
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));

  // assert(reinterpret_cast<Page *>(bucket_page)->GetPinCount() == 0);
  // assert(reinterpret_cast<Page *>(dir_page)->GetPinCount() == 0);

  table_latch_.WUnlock();
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

  // table_latch_.RLock();
  table_latch_.WLock();

  /// FIXME(bayes): also risky on no spare frames.
  auto *dir_page = FetchDirectoryPage();
  const page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  found_key = bucket_page->GetValue(key, comparator_, result);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

  // assert(reinterpret_cast<Page *>(bucket_page)->GetPinCount() == 0);
  // assert(reinterpret_cast<Page *>(dir_page)->GetPinCount() == 0);

  // table_latch_.RUnlock();
  table_latch_.WUnlock();

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
  // table_latch_.RLock();
  table_latch_.WLock();

  /// FIXME(bayes): also risky on no spare frames.
  auto *dir_page = FetchDirectoryPage();
  const page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);
  assert(bucket_page != nullptr);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  const bool is_full = bucket_page->IsFull();
  const bool has_duplicate = bucket_page->HasDuplicate(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  // if a duplicate has been found, reject this insertion.
  if (has_duplicate) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    // table_latch_.RUnlock();
    table_latch_.WUnlock();

    return false;
  }

  /// FIXME(bayes): what if another thread inserts a key into the bucket at this time and makes it full?

  // if not full, a trivial insertion.
  if (!is_full) {
    reinterpret_cast<Page *>(bucket_page)->WLatch();
    const bool dirty_flag = bucket_page->Insert(key, value, comparator_);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();

    // flush the dirty page immediately.
    if (dirty_flag) {
      assert(buffer_pool_manager_->FlushPage(bucket_page_id));
    }
    buffer_pool_manager_->UnpinPage(bucket_page_id, dirty_flag);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, dirty_flag));
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    // table_latch_.RUnlock();
    table_latch_.WUnlock();

    return dirty_flag;
  }

  // otherwise, has to do bucket splitting and potential directory expansion.

  /// FIXME(bayes): Is it safe to switch lock modes in this way? We need lock promotion!!!
  // table_latch_.RUnlock();
  // table_latch_.WLock();

  // request a new page to be used as the split image.
  page_id_t split_img_id;
  if (buffer_pool_manager_->NewPage(&split_img_id) == nullptr) {
    // buffer pool has no spare frames currently.

    // unpin pages.
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    table_latch_.WUnlock();

    return false;
  }
  assert(buffer_pool_manager_->FlushPage(split_img_id));  // flush the new page immediately.
  assert(buffer_pool_manager_->UnpinPage(split_img_id, true));

  /// FIXME(bayes): also risky on no spare frames.
  auto *split_img = FetchBucketPage(split_img_id);
  assert(split_img != nullptr);

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

    // //! debug.
    dir_page->VerifyIntegrity();
  }

  // collect all linked directory entries with the overflowing bucket page.
  std::vector<uint32_t> linked_entries;
  linked_entries.reserve(dir_page->Size());
  for (uint32_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      linked_entries.push_back(i);
      // increment their local depths BTW.
      dir_page->IncrLocalDepth(i);
    }
  }
  assert(!linked_entries.empty());

  // relink according to the mask result between the directory index and the high bit.
  // those with high bit 0 are linked with the overflowing bucket page.
  // those with high bit 1 are linked with the split image.
  const uint32_t high_bit = dir_page->GetLocalHighBit(dir_idx);
  for (const uint32_t &i : linked_entries) {
    if ((i & high_bit) != 0) {
      dir_page->SetBucketPageId(i, split_img_id);
    }
  }
  // //! debug.
  dir_page->VerifyIntegrity();

  // reinsert the key-value pairs in the overflowing bucket page. Also by inspecting the high bit.
  // those with high bit 0 stay in the overflowing bucket page.
  // those with high bit 1 are removed from the overflowing bucket page and reinserted to the split image.
  /// FIXME(bayes): Is it necessary to lock split_img as well?
  bool move_occur{false};  // Did data moving happen?
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  reinterpret_cast<Page *>(split_img)->WLatch();
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    const uint32_t key_hash = Hash(bucket_page->KeyAt(i));
    if ((key_hash & high_bit) != 0) {
      assert(split_img->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), comparator_));
      bucket_page->RemoveAt(i);
      move_occur |= true;
    }
  }
  reinterpret_cast<Page *>(split_img)->WUnlatch();
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  // flush pages.
  if (move_occur) {
    assert(buffer_pool_manager_->FlushPage(bucket_page_id));
  }
  assert(buffer_pool_manager_->FlushPage(split_img_id));  // flush new page immediately.
  assert(buffer_pool_manager_->FlushPage(directory_page_id_));

  // unpin pages.
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, move_occur);
  buffer_pool_manager_->UnpinPage(split_img_id, true);  // marked as dirty to be persistent.
  // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, move_occur));
  // assert(buffer_pool_manager_->UnpinPage(split_img_id, true));  // marked as dirty to be persistent.

  table_latch_.WUnlock();

  // assert(reinterpret_cast<Page *>(split_img)->GetPinCount() == 0);
  // assert(reinterpret_cast<Page *>(bucket_page)->GetPinCount() == 0);
  // assert(reinterpret_cast<Page *>(dir_page)->GetPinCount() == 0);

  // retry insertion.
  return SplitInsert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.RLock();
  table_latch_.WLock();

  /// FIXME(bayes): also risky on no spare frames.
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
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    // table_latch_.RUnlock();
    table_latch_.WUnlock();
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
    assert(buffer_pool_manager_->FlushPage(bucket_page_id));
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    table_latch_.WUnlock();
    // table_latch_.RUnlock();
    return true;
  }
  // has to do bucket merging.

  // table_latch_.RUnlock();
  // table_latch_.WLock();

  Merge(transaction, key, value);

  // unpin the bucket page.
  assert(buffer_pool_manager_->FlushPage(bucket_page_id));
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));

  // mergings may incur directory shrinkings.
  while (dir_page->CanShrink()) {
    // shrinking is trivial since the lower half of the directory array can be safely cut out.

    // reset the lower half.
    for (uint32_t i = dir_page->Size() / 2; i < dir_page->Size(); ++i) {
      dir_page->SetBucketPageId(i, INVALID_PAGE_ID);
      dir_page->SetLocalDepth(i, 0);
    }

    // decrement the global depth.
    dir_page->DecrGlobalDepth();
  }

  // unpin the directory page.
  assert(buffer_pool_manager_->FlushPage(directory_page_id_));
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));

  table_latch_.WUnlock();

  // assert(reinterpret_cast<Page *>(bucket_page)->GetPinCount() == 0);
  // assert(reinterpret_cast<Page *>(dir_page)->GetPinCount() == 0);

  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // no need to hold latch since the caller Remove must hold it.

  /// FIXME(bayes): also risky on no spare frames.
  auto *dir_page = FetchDirectoryPage();

  // check if there's only one bucket.
  if (dir_page->Size() == 1) {
    // only one bucket. Cannot merge.
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    return;
  }

  // find buddy directory entries
  const uint32_t dir_idx = KeyToDirectoryIndex(key, dir_page);
  const uint32_t buddy_idx = (dir_idx ^ dir_page->GetLocalHighBit(dir_idx));

  // if no buddy entry found, i.e. there's only one bucket currently. Cannot merge.
  if (buddy_idx > dir_page->Size()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    return;
  }

  // find buddy bucket pages.
  const page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  const page_id_t split_img_id = dir_page->GetBucketPageId(buddy_idx);
  auto *bucket_page = FetchBucketPage(bucket_page_id);
  auto *split_img = FetchBucketPage(split_img_id);
  // assert(bucket_page_id != split_img_id);
  // assert(bucket_page != nullptr && split_img != nullptr);

  // check merging conditions.
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  reinterpret_cast<Page *>(split_img)->RLatch();
  // (1) both're empty.
  const bool cond0 = (bucket_page->IsEmpty() && split_img->IsEmpty());
  reinterpret_cast<Page *>(split_img)->RUnlatch();
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  // (2) of the same local depth.
  const bool cond1 = (dir_page->GetLocalDepth(dir_idx) == dir_page->GetLocalDepth(buddy_idx));
  // (3) and the local depths are not 0.
  const bool cond2 = (dir_page->GetLocalDepth(dir_idx) > 0);

  if (!(cond0 && cond1 && cond2)) {
    // some conditions are violated. Cannot merge.

    // unpin pages.
    buffer_pool_manager_->UnpinPage(split_img_id, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    // assert(buffer_pool_manager_->UnpinPage(split_img_id, false));
    // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

    return;
  }
  // all conditions are satisfied, do merging.

  // make all the entries, that pointed to the split image, point to the bucket page.
  for (uint32_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == split_img_id) {
      dir_page->SetBucketPageId(i, bucket_page_id);
    }
  }

  // for each directory entry that is linked with the bucket page, decrement its local depth.
  for (uint32_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      dir_page->DecrLocalDepth(i);
    }
  }
  //! no need to move key-value pairs, since the split image is empty.

  // unpin and delete the split image.
  buffer_pool_manager_->UnpinPage(split_img_id, false);  // the split image is untouch during the merging.
  buffer_pool_manager_->DeletePage(split_img_id);
  // assert(buffer_pool_manager_->UnpinPage(split_img_id, false));  // the split image is untouch during the merging.
  // assert(buffer_pool_manager_->DeletePage(split_img_id));

  // unpin the directory page and the bucket page.
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  assert(buffer_pool_manager_->FlushPage(directory_page_id_));
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));

  // assert(reinterpret_cast<Page *>(split_img)->GetPinCount() == 0);
  // //! dir_page and bucket page are pinned by Remove.
  // assert(reinterpret_cast<Page *>(bucket_page)->GetPinCount() == 1);
  // assert(reinterpret_cast<Page *>(dir_page)->GetPinCount() == 1);

  // since the local depth has been decremented, the bucket page's split image is changed.
  // so there might be another merging on this bucket page.
  Merge(transaction, key, value);
}

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
