//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include "common/exception.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  auto guard = std::lock_guard(latch_);
  auto evict_list = std::vector<std::tuple<frame_id_t, size_t, size_t>>{};
  for (const auto &[fid, node] : node_store_) {
    if (node.is_evictable_) {
      if (node.history_.size() < k_) {
        evict_list.push_back({fid, std::numeric_limits<size_t>::max(), node.history_.front()});
      } else {
        evict_list.push_back({fid, current_timestamp_ - node.history_.front(), node.history_.front()});
      }
    }
  }
  if (evict_list.empty()) {
    return false;
  }
  std::sort(evict_list.begin(), evict_list.end(), [](const auto &lhs, const auto &rhs) {
    if (std::get<1>(lhs) == std::get<1>(rhs)) {
      return std::get<2>(lhs) < std::get<2>(rhs);
    }
    return std::get<1>(lhs) > std::get<1>(rhs);
  });
  *frame_id = std::get<0>(evict_list.front());
  auto &node = node_store_[*frame_id];

  node.history_.clear();
  node.is_evictable_ = false;
  node_store_.erase(*frame_id);
  --curr_size_;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  auto guard = std::lock_guard(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("frame_id is out of range");
    return;
  }
  auto node = node_store_.find(frame_id);
  if (node == node_store_.end()) {
    node_store_.insert({frame_id, LRUKNode{}});
  } else {
    if (node->second.history_.size() >= k_) {
      node->second.history_.pop_front();
    }
  }
  node_store_[frame_id].history_.push_back(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto guard = std::lock_guard(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_ || node_store_.count(frame_id) == 0) {
    throw std::out_of_range("frame_id is invalid");
    return;
  }
  bool old_evictable = node_store_[frame_id].is_evictable_;
  if (set_evictable != old_evictable) {
    if (set_evictable == true) {
      ++curr_size_;
    } else {
      --curr_size_;
    }
    node_store_[frame_id].is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto guard = std::lock_guard(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("frame_id is invalid");
    return;
  }
  if (node_store_.count(frame_id) == 0) {
    return;
  } else {
    auto &node = node_store_[frame_id];
    if (node.is_evictable_ == false) {
      throw std::logic_error("frame_id is not evictable");
      return;
    } else {
      node.history_.clear();
      node.is_evictable_ = false;
      node_store_.erase(frame_id);
      --curr_size_;
    }
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
