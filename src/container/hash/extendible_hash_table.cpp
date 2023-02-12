//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto bucket = std::make_shared<Bucket>(bucket_size);
  dir_.emplace_back(bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  int newDepth = bucket->GetDepth() + 1;
  size_t baseMask = (1 << bucket->GetDepth()) - 1;
  size_t splitMask = (1 << newDepth) - 1;
  auto firstBucket = std::make_shared<Bucket>(bucket_size_, newDepth);
  auto secondBucket = std::make_shared<Bucket>(bucket_size_, newDepth);
  std::list<std::pair<K, V>> items = bucket->GetItems();
  size_t lowIndex = IndexOf(items.begin()->first) & baseMask;
  for (auto ele = items.begin(); ele != items.end(); ele++) {
    size_t splitIndex = IndexOf(ele->first);
    if ((splitIndex & splitMask) == lowIndex) {
      firstBucket->GetItems().emplace_back(*ele);
      continue;
    }
    secondBucket->GetItems().emplace_back(*ele);
  }
  for (size_t i = 0; i < dir_.size(); i++) {
    if ((i & baseMask) == lowIndex) {
      if ((i & splitMask) == lowIndex) {
        dir_[i] = firstBucket;
        continue;
      }
      dir_[i] = secondBucket;
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];
  bool res = bucket->Insert(key, value);
  if (res) {
    return;
  }

  // Bucket is full ,but the depth of bucket less than local depth, split the bucket.
  if (GetGlobalDepth() > bucket->GetDepth()) {
    RedistributeBucket(bucket);
    return Insert(key, value);
  }

  // Bucket is full, depth of bucket equals global depth, extend the dir and split the bucket.
  int dirMask = (1 << global_depth_) - 1;
  global_depth_++;
  std::vector<std::shared_ptr<Bucket>> newDir(1 << global_depth_);
  for (size_t i = 0; i < (size_t)(1 << global_depth_); i++) {
    size_t oldIndex = i & dirMask;
    newDir[i] = dir_[oldIndex];
  }
  dir_ = newDir;
  RedistributeBucket(bucket);
  return Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto ele = list_.begin(); ele != list_.end(); ele++) {
    if (ele->first == key) {
      value = ele->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto ele = list_.begin(); ele != list_.end();) {
    std::pair<K, V> entry = *ele;
    if (entry.first == key) {
      list_.erase(ele);
      return true;
    }
    ele++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }

  // if the key already exists, overwrite its value with new value
  for (auto ele = list_.begin(); ele != list_.end(); ele++) {
    std::pair<K, V> entry = *ele;
    if (entry.first == key) {
      entry.second = value;
      return true;
    }
  }

  // Here the local depth of the bucket is less than global depth.
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
