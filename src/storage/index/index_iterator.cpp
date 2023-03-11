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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, LeafPage *leaf, int index)
    : bpm_(bpm), leaf_(leaf), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { bpm_->UnpinPage(leaf_->GetPageId(), false); }  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (leaf_->GetNextPageId() == INVALID_PAGE_ID) && (index_ == (leaf_->GetSize() - 1));
}

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  std::cout << "Get iterator value." << std::endl;
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  std::cout << "Get next operator." << std::endl;
  if (index_ == (leaf_->GetSize() - 1) && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t page_id = leaf_->GetNextPageId();
    bpm_->UnpinPage(leaf_->GetPageId(), false);
    Page *page = bpm_->FetchPage(page_id);
    leaf_ = reinterpret_cast<LeafPage *>(page->GetData());
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
