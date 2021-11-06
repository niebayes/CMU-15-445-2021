//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

#define MAX_SIZE (BUCKET_ARRAY_SIZE)
#define BYTE_SIZE (8)

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (uint32_t i = 0; i < MAX_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && (cmp(key, KeyAt(i)) == 0)) {
      result->push_back(ValueAt(i));
    }
  }
  // check if found at least one key-value pair.
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull()) {
    return false;
  }

  for (uint32_t i = 0; i < MAX_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && (cmp(key, KeyAt(i)) == 0) && value == ValueAt(i)) {
      // found a duplicate key-value pair. Reject the insertion.
      return false;
    }
  }
  // no duplicate key-value pair found.

  for (uint32_t i = 0; i < MAX_SIZE; ++i) {
    if (!IsReadable(i)) {
      // found a tombstone or an empty slot. Insert at here.
      SetReadable(i);
      array_[i] = MappingType{key, value};
      return true;
    }
  }

  // cannot reach here.
  assert(false);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t i = 0; i < MAX_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && (cmp(key, KeyAt(i)) == 0) && value == ValueAt(i)) {
      // found the target. Remove it.
      RemoveAt(i);
      return true;
    }
  }
  // the target was not found.
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  /// FIXME(bayes): double RemoveAt unexpected set to readable.
  readable_[bucket_idx / BYTE_SIZE] ^= (1 << (bucket_idx % BYTE_SIZE));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return ((occupied_[bucket_idx / BYTE_SIZE] & (1 << (bucket_idx % BYTE_SIZE))) != 0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  /// FIXME(bayes): double SetOccupied unexpected set to not-occupied.
  occupied_[bucket_idx / BYTE_SIZE] ^= (1 << (bucket_idx % BYTE_SIZE));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return ((readable_[bucket_idx / BYTE_SIZE] & (1 << (bucket_idx % BYTE_SIZE))) != 0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  /// FIXME(bayes): double SetReadable unexpected set to not-readable.
  readable_[bucket_idx / BYTE_SIZE] ^= (1 << (bucket_idx % BYTE_SIZE));
  SetOccupied(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return (NumReadable() == MAX_SIZE);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t num_readables{0};
  for (uint32_t i = 0; i < MAX_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    num_readables += IsReadable(i);
  }
  return num_readables;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return (NumReadable() == 0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
