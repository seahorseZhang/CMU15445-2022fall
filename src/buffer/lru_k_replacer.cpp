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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  for (auto iter = temp_pool_.begin(); iter != temp_pool_.end();) {
    if ((*iter)->IsEvictable()) {
      *frame_id = (*iter)->GetId();
      iter = temp_pool_.erase(iter);
      return true;
    }
    iter++;
  }
  for (auto iter = cache_pool_.begin(); iter != cache_pool_.end();) {
    if ((*iter)->IsEvictable()) {
      *frame_id = (*iter)->GetId();
      iter = cache_pool_.erase(iter);
      return true;
    }
    iter++;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  for (auto iter = cache_pool_.begin(); iter != cache_pool_.end();) {
    if ((*iter)->GetId() == frame_id) {
      cache_pool_.emplace_back(std::move(*iter));
      iter = cache_pool_.erase(iter);
      return;
    }
    iter++;
  }

  for (auto iter = temp_pool_.begin(); iter != temp_pool_.end();) {
    if ((*iter)->GetId() == frame_id) {
      (*iter)->IncreaseTimes();
      if (((*iter)->GetTimes()) >= k_) {
        cache_pool_.emplace_back(std::move(*iter));
        iter = temp_pool_.erase(iter);
        return;
      }
    }
    iter++;
  }
  std::unique_ptr<FrameInfo> frame_ptr = std::make_unique<FrameInfo>(frame_id);
  frame_ptr->IncreaseTimes();
  temp_pool_.push_back(std::move(frame_ptr));
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  for (auto &ele : temp_pool_) {
    if (ele->GetId() == frame_id) {
      ele->SetEvictable(set_evictable);
      return;
    }
  }
  for (auto &ele : cache_pool_) {
    if (ele->GetId() == frame_id) {
      ele->SetEvictable(set_evictable);
      return;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  for (auto &ele : temp_pool_) {
    if (ele->GetId() == frame_id) {
      if (ele->IsEvictable()) {
        temp_pool_.remove(ele);
        return;
      }
      throw frame_id;
    }
  }
  for (auto &ele : cache_pool_) {
    if (ele->GetId() == frame_id) {
      if (ele->IsEvictable()) {
        cache_pool_.remove(ele);
        return;
      }
      throw frame_id;
    }
  }
}

auto LRUKReplacer::Size() -> size_t {
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
