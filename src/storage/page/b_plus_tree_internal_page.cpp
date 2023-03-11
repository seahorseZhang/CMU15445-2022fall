//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair1, auto key) {
    return comparator(pair1.first, key) < 0;
  });
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  return std::prev(target)->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *dst_page, BufferPoolManager *bpm) -> void {
  int new_size = GetMinSize();
  dst_page->CopyData(array_ + new_size, GetSize() - new_size, bpm);
  SetSize(new_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *dst_page, BufferPoolManager *bpm) -> void {
  dst_page->CopyData(array_, GetSize(), bpm);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyData(MappingType *items, int size, BufferPoolManager *bpm) -> void {
  std::copy(items, items + size, array_);
  IncreaseSize(size);
  for (int index = 0; index < size; index++) {
    Page *page = bpm->FetchPage(ValueAt(index));
    auto *internal = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    internal->SetParentPageId(GetPageId());
    bpm->UnpinPage(page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) -> void {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToEnd(const KeyType &key, const ValueType &value, BufferPoolManager *bpm) {
  int size = GetSize();
  array_[size] = {key, value};
  IncreaseSize(1);
  auto page_id = reinterpret_cast<page_id_t>(value);
  Page *page = bpm->FetchPage(page_id);
  auto *b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  b_plus_page->SetParentPageId(GetPageId());
  bpm->UnpinPage(page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToStart(const KeyType &key, const ValueType &value, BufferPoolManager *bpm) {
  int size = GetSize();
  std::move_backward(array_, array_ + size, array_ + 1);
  array_[0] = {key, value};
  IncreaseSize(1);
  auto page_id = reinterpret_cast<page_id_t>(value);
  Page *page = bpm->FetchPage(page_id);
  auto *b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  b_plus_page->SetParentPageId(GetPageId());
  bpm->UnpinPage(page_id, true);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto iter = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, iter);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(page_id_t new_page_id, const KeyType &key, page_id_t old_page_id) {
  int index = ValueIndex(old_page_id) + 1;
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  IncreaseSize(1);
  array_[index].first = key;
  array_[index].second = new_page_id;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
