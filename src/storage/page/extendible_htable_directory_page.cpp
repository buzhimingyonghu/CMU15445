//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::fill(local_depths_, local_depths_ + HTABLE_DIRECTORY_ARRAY_SIZE, 0);
  std::fill(bucket_page_ids_, bucket_page_ids_ + HTABLE_DIRECTORY_ARRAY_SIZE, INVALID_PAGE_ID);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << local_depths_[bucket_idx]) - 1;
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }
auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  for (uint32_t idx = 0; idx < Size(); ++idx) {
    auto split_image_idx = idx | (1 << global_depth_);
    bucket_page_ids_[split_image_idx] = bucket_page_ids_[idx];
    local_depths_[split_image_idx] = local_depths_[idx];
  }
  ++global_depth_;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { --global_depth_; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  return std::find_if(local_depths_, local_depths_ + Size(),
                      [this](uint8_t local_depth) { return local_depth == global_depth_; }) == local_depths_ + Size();
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { ++local_depths_[bucket_idx]; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { --local_depths_[bucket_idx]; }

}  // namespace bustub
