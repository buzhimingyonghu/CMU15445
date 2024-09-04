//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  auto header_guard = bpm->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto header = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_guard.GetDataMut());
  header->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 Transaction *transaction) const -> bool {
  auto hash = Hash(key);

  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  const auto header = reinterpret_cast<const ExtendibleHTableHeaderPage *>(header_guard.GetData());

  auto directory_page_id = header->GetDirectoryPageId(header->HashToDirectoryIndex(hash));
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  const auto directory = reinterpret_cast<const ExtendibleHTableDirectoryPage *>(directory_guard.GetData());

  auto bucket_page_id = directory->GetBucketPageId(directory->HashToBucketIndex(hash));
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  const auto bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetData());

  for (size_t i = 0; i < bucket->Size(); i++) {
    if (cmp_(key, bucket->KeyAt(i)) == 0) {
      result->push_back(bucket->ValueAt(i));
    }
  }

  return !result->empty();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_guard.GetDataMut());

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }

  header_guard.Drop();

  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  while (true) {
    auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());

    if (!bucket->IsFull()) {
      return bucket->Insert(key, value, cmp_);
    }

    if (directory->GetLocalDepth(bucket_idx) == directory->GetMaxDepth()) {
      return false;
    }

    if (directory->GetLocalDepth(bucket_idx) == directory->GetGlobalDepth()) {
      directory->IncrGlobalDepth();
    }
    directory->IncrLocalDepth(bucket_idx);

    auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    auto new_bucket_page_id = INVALID_PAGE_ID;
    auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    if (new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }

    auto new_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(new_bucket_guard.GetDataMut());

    new_bucket->Init(bucket_max_size_);
    directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

    auto local_depth_mask = directory->GetLocalDepthMask(bucket_idx);
    MigrateEntries(bucket, new_bucket, new_bucket_idx, local_depth_mask);

    directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
    directory->SetLocalDepth(new_bucket_idx, directory->GetLocalDepth(bucket_idx));

    UpdateDirectoryMapping(directory, bucket_idx, bucket_page_id, directory->GetLocalDepth(bucket_idx),
                           local_depth_mask);
    UpdateDirectoryMapping(directory, new_bucket_idx, new_bucket_page_id, directory->GetLocalDepth(new_bucket_idx),
                           local_depth_mask);

    if ((hash & local_depth_mask) == new_bucket_idx) {
      bucket_page_id = new_bucket_page_id;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());

  directory->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);

  auto bucket_idx = directory->HashToBucketIndex(hash);
  return InsertToNewBucket(directory, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());

  bucket->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);

  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  for (size_t i = 0; i < directory->Size(); ++i) {
    if ((i & local_depth_mask) == new_bucket_idx) {
      directory->SetBucketPageId(i, new_bucket_page_id);
      directory->SetLocalDepth(i, new_local_depth);
    }
  }
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  auto removed_keys = std::vector<K>();
  for (size_t i = 0; i < old_bucket->Size(); i++) {
    auto key = old_bucket->KeyAt(i);
    auto value = old_bucket->ValueAt(i);
    if ((Hash(key) & local_depth_mask) == new_bucket_idx) {
      new_bucket->Insert(key, value, cmp_);
      removed_keys.emplace_back(key);
    }
  }

  for (auto &key : removed_keys) {
    old_bucket->Remove(key, cmp_);
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = Hash(key);

  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = reinterpret_cast<const ExtendibleHTableHeaderPage *>(header_guard.GetData());

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  header_guard.Drop();

  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = reinterpret_cast<ExtendibleHTableDirectoryPage *>(directory_guard.GetDataMut());

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  {
    auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());

    auto removed = bucket->Remove(key, cmp_);
    if (!removed) {
      return false;
    }
  }

  while (true) {
    auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
    if (directory->GetLocalDepth(bucket_idx) == 0) {
      return true;
    }

    auto split_image_idx = directory->GetSplitImageIndex(bucket_idx);
    auto split_image_page_id = directory->GetBucketPageId(split_image_idx);
    if (split_image_page_id == INVALID_PAGE_ID) {
      return true;
    }

    if (directory->GetLocalDepth(split_image_idx) != directory->GetLocalDepth(bucket_idx)) {
      return true;
    }

    auto split_image_guard = bpm_->FetchPageWrite(split_image_page_id);
    auto split_image = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(split_image_guard.GetDataMut());

    if (!bucket->IsEmpty() && !split_image->IsEmpty()) {
      return true;
    }

    directory->DecrLocalDepth(bucket_idx);
    directory->DecrLocalDepth(split_image_idx);
    if (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }

    auto new_bucket_idx = directory->HashToBucketIndex(hash);
    auto new_bucket_page_id = directory->GetBucketPageId(new_bucket_idx);

    auto local_depth_mask = directory->GetLocalDepthMask(new_bucket_idx);

    if (new_bucket_idx == bucket_idx) {
      MigrateEntries(split_image, bucket, new_bucket_idx, local_depth_mask);
      bpm_->DeletePage(split_image_page_id);
    } else {
      MigrateEntries(bucket, split_image, new_bucket_idx, local_depth_mask);
      bpm_->DeletePage(bucket_page_id);
    }

    UpdateDirectoryMapping(directory, new_bucket_idx, new_bucket_page_id, directory->GetLocalDepth(new_bucket_idx),
                           local_depth_mask);

    bucket_page_id = new_bucket_page_id;
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
