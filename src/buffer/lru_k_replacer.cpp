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
#include "iostream"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  for (auto iter = temp_pool_.begin(); iter != temp_pool_.end();) {
    if ((*iter)->IsEvictable()) {
      *frame_id = (*iter)->GetId();
      iter = temp_pool_.erase(iter);
      temp_map_.erase(*frame_id);
      latch_.unlock();
      return true;
    }
    iter++;
  }
  for (auto iter = cache_pool_.begin(); iter != cache_pool_.end();) {
    if ((*iter)->IsEvictable()) {
      *frame_id = (*iter)->GetId();
      iter = cache_pool_.erase(iter);
      cache_map_.erase(*frame_id);
      latch_.unlock();
      return true;
    }
    iter++;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "Invalid frame id");
  latch_.lock();
  if (cache_map_.count(frame_id) != 0U) {
    auto iter = cache_map_[frame_id];
    cache_pool_.emplace_back(std::move(*iter));
    cache_pool_.erase(iter);
    cache_map_[frame_id] = (--cache_pool_.end());
    latch_.unlock();
    return;
  }

  if (temp_map_.count(frame_id) != 0U) {
    auto iter = temp_map_[frame_id];
    (*iter)->IncreaseTimes();
    if ((*iter)->GetTimes() >= k_) {
      cache_pool_.emplace_back(std::move(*iter));
      cache_map_[frame_id] = (--cache_pool_.end());
      temp_pool_.erase(iter);
      temp_map_.erase(frame_id);
    }
    latch_.unlock();
    return;
  }

  std::unique_ptr<FrameInfo> frame_ptr = std::make_unique<FrameInfo>(frame_id);
  frame_ptr->IncreaseTimes();
  temp_pool_.emplace_back(std::move(frame_ptr));
  temp_map_[frame_id] = (--temp_pool_.end());
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (temp_map_.count(frame_id) != 0U) {
    auto iter = temp_map_[frame_id];
    (*iter)->SetEvictable(set_evictable);
    latch_.unlock();
    return;
  }

  if (cache_map_.count(frame_id) != 0U) {
    auto iter = cache_map_[frame_id];
    (*iter)->SetEvictable(set_evictable);
    latch_.unlock();
    return;
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (temp_map_.count(frame_id) != 0U) {
    auto iter = temp_map_[frame_id];
    BUSTUB_ASSERT((*iter)->IsEvictable(), "Remove unEvictable frame id.");
    temp_pool_.erase(iter);
    temp_map_.erase(frame_id);
    latch_.unlock();
    return;
  }
  if (cache_map_.count(frame_id) != 0U) {
    auto iter = cache_map_[frame_id];
    BUSTUB_ASSERT((*iter)->IsEvictable(), "Remove unEvictable frame id.");
    cache_pool_.erase(iter);
    cache_map_.erase(frame_id);
    latch_.unlock();
    return;
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t num = 0;
  for (auto &ele : cache_pool_) {
    if (ele->IsEvictable()) {
      num++;
    }
  }
  for (auto &ele : temp_pool_) {
    if (ele->IsEvictable()) {
      num++;
    }
  }
  return num;
}

LRUKReplacer::FrameInfo::FrameInfo(frame_id_t frame_id) : frame_id_(frame_id) {
  times_ = 0;
  evictable_ = true;
}

}  // namespace bustub
