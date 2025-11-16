/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int index)
    : bpm_(bpm), page_(page), index_(index) {
      leaf_ = reinterpret_cast<LeafPage *>(page->GetData());
    }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), false);
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (leaf_->GetNextPageId() == INVALID_PAGE_ID) && (index_ >= leaf_->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator*() -> const MappingType {
  assert(leaf_ != nullptr);
  page_->RLatch();
  assert(index_ < leaf_->GetSize());
  const MappingType result = leaf_->GetItem(index_);
  page_->RUnlatch();
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  page_->RLatch();
  if (index_ == (leaf_->GetSize() - 1)) {
    if (leaf_->GetNextPageId() != INVALID_PAGE_ID) {
      page_id_t page_id = leaf_->GetNextPageId();
      page_->RUnlatch();
      bpm_->UnpinPage(leaf_->GetPageId(), false);
      page_ = bpm_->FetchPage(page_id);
      leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
      index_ = 0;
    } else {
       ++index_;
       page_->RUnlatch();
    }
  } else {
      ++index_;
      page_->RUnlatch();
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(INDEXITERATOR_TYPE &&other)
    : bpm_(other.bpm_), page_(other.page_), leaf_(other.leaf_), index_(other.index_) {
  other.page_ = nullptr;
  other.leaf_ = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator=(INDEXITERATOR_TYPE &&other)  {
  if (this != &other) {
    bpm_ = other.bpm_;
    page_ = other.page_;
    leaf_ = other.leaf_;
    index_ = other.index_;
    other.page_ = nullptr;
    other.leaf_ = nullptr;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
