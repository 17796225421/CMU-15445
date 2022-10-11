//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 1. 循环，直到遇到叶子节点，当前节点是内部节点
  // 2. 当前节点kv数组使用二分找到最后一个小于等于key的k，获得儿子节点页编号
  // 3. 通过儿子节点页编号，用数据库缓冲池获取页节点

  Page *leaf_page = FindLeafPageByOperation(key, Operation::FIND, transaction).first;

  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData()); 

  ValueType value{};
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);

  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page

  if (!is_exist) {
    return false;
  }
  result->push_back(value);
  return true;

}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 1. 下降，找到叶节点
  // 2. 叶节点kv数组二分找到第一个大于等于key的k
  // 3. 将后续所有kv对往后挪，插入kv对
  // 4. 如果kv对数组大小大于等于容量，分割后上升

  {
    const std::lock_guard<std::mutex> guard(root_latch_);  // 注意新建根节点时要锁住
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }
  return InsertIntoLeaf(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *root_page = buffer_pool_manager_->NewPage(&new_page_id);  
  if (nullptr == root_page) {
    throw std::runtime_error("out of memory");
  }
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);  

  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());  
  root_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);            
  root_node->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);  

}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::INSERT, transaction);

  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData()); 

  int size = leaf_node->GetSize();

  int new_size = leaf_node->Insert(key, value, comparator_);

  if (new_size == size) {
    if (root_is_latched) {
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction); 
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page
    return false;
  }

  if (new_size < leaf_node->GetMaxSize()) {

    if (root_is_latched) {
      root_latch_.unlock();
    }

    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // unpin leaf page
    return true;
  }

  LeafPage *new_leaf_node = Split(leaf_node);  

  bool *pointer_root_is_latched = new bool(root_is_latched);

  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction,
                   pointer_root_is_latched); 

  assert((*pointer_root_is_latched) == false);

  delete pointer_root_is_latched;

  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);      // unpin leaf page
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);  

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  
  if (nullptr == new_page) {
    throw std::runtime_error("out of memory");
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData()); 
  new_node->SetPageType(node->GetPageType());               

  if (node->IsLeafPage()) { 
    LeafPage *old_leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);  
    old_leaf_node->MoveHalfTo(new_leaf_node);
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());  
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());      
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else { 
    InternalPage *old_internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(new_node);
    new_internal_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);  
    old_internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  return new_node;  
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool *root_is_latched) {
  // 1. 缓冲池创建叶节点，作为新节点
  // 2. 当前节点kv数组右半部分转移到新节点kv数组
  // 3. 递归，往当前节点的父节点kv数组插入新节点kv数组的下标0的k
  // 4. 递归终点，当前节点是根节点
  //   a. 缓冲池创建页节点作为当前节点父节点，也作为真正根节点
  //   b. 根节点kv数组下标0是当前节点编号，下标1是新节点编号，结束递归
  // 5. 缓冲池获取页节点，是当前节点的父节点
  // 6. 父节点kv数组用二分找到第一个大于等于key的k，key是新节点kv数组下标0的k
  // 7. 后序kv往后挪，插入新节点kv数组下标0的k，v是新节点编号。
  // 8. 如果父节点kv数组未满，结束
  // 9. 父节点kv数组满了，递归调用分割并上升。

  if (old_node->IsRootPage()) {  
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id); 
    root_page_id_ = new_page_id;

    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_); 
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);

    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);  // 修改了new_page->data，所以dirty置为true

    UpdateRootPageId(0);  // update root page id in header page

    // 新的root必定不在transaction的page_set_队列中
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);
    return;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());  // pin parent page

  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  parent_node->Insert NodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  // size+1

  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction); 
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // unpin parent page
    return;
  }

  InternalPage *new_parent_node = Split(parent_node);  // pin new parent node
  InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction, root_is_latched);

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);      // unpin parent page
  buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);  // unpin new parent node
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 1. 下降，找到叶节点
  // 2. 对叶节点kv数组二分找到k等于key
  // 3. 后序kv对往前挪
  // 4. 当前节点kv数组大于等于容量一半，结束
  // 5. 当前节点kv数组小于容量一半，索取或合并后上升

  if (IsEmpty()) {
    return;
  }
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::DELETE, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);  // 在leaf中删除key（如果不存在该key，则size不变）

  if (new_size == old_size) {
    if (root_is_latched) {
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction);

    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page

    return;
  }

  bool *pointer_root_is_latched = new bool(root_is_latched);

  bool leaf_should_delete = CoalesceOrRedistribute(leaf_node, transaction, pointer_root_is_latched);
  assert((*pointer_root_is_latched) == false);

  delete pointer_root_is_latched;

  if (leaf_should_delete) {
    transaction->AddIntoDeletedPageSet(leaf_page->GetPageId());
  }


  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // unpin leaf page

  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool *root_is_latched) {
  // 1. 递归终点，当前节点是根节点，根节点允许kv数组小于容量一半，直接返回
  // 2. 缓冲池获取节点，是当前节点的父节点
  // 3. 父节点二分找到当前节点当前kv对
  // 4. 找到当前kv对前一个kv对，进而找到当前节点的左边节点
  //   a. 如果当前kv对是父节点kv数组下标0，找到当前kv对后一个kv对，进而找到当前节点的右边节点
  // 5. 如果左边节点kv数组大小大于容量一半，可以索取
  //   a. 左边节点最后一个kv转移到当前节点下标0处，当前节点原来kv对往后挪
  //   b. 更新父节点对当前节点的kv对的k。
  // 6. 如果左边节点kv数组大小等于容量一半，必须合并
  // 7. 将当前节点所有kv转移到左边节点kv数组末尾
  // 8. 如果是叶子节点，更新左边节点的next页编号
  // 9. 父节点二分找到当前节点的kv对，删除
  // 10. 当前节点用缓冲池的删除节点来删除
  // 11. 递归父节点进行索引或合并后上升

  if (node->IsRootPage()) {
    bool root_should_delete = AdjustRoot(node);

    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);
    return root_should_delete;  
  }

  if (node->GetSize() >= node->GetMinSize()) {

    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);
    return false;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  int index = parent->ValueIndex(node->GetPageId());
  page_id_t sibling_page_id = parent->ValueAt(index == 0 ? 1 : index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);

  sibling_page->WLatch();  

  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    Redistribute(sibling_node, node, index);  

    UnlockPages(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

    return false;  
  }

  bool parent_should_delete =
      Coalesce(&sibling_node, &node, &parent, index, transaction, root_is_latched);  // 返回值是parent是否需要被删除

  assert((*root_is_latched) == false);

  if (parent_should_delete) {
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

  return true; 
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool *root_is_latched) {
  int key_index = index;
  if (index == 0) {
    std::swap(neighbor_node, node);  
    key_index = 1;
  }
  KeyType middle_key = (*parent)->KeyAt(key_index);

  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(neighbor_leaf_node);
    neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
  }

  (*parent)->Remove(key_index);

  return CoalesceOrRedistribute(*parent, transaction, root_is_latched);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());  // parent of node

  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else { 
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) { 
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), buffer_pool_manager_);
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {  
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();

    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);

    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return true;
  }
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);

    return true;
  }
  // 否则不需要有page被删除，直接返回false
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, true).first;
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, 0);  // 最左边的叶子且index=0
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *leaf_page = FindLeafPageByOperation(key, Operation::FIND).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);  // 此处直接用KeyIndex，而不是Lookup
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  // LOG_INFO("Enter tree.end()");
  // find leftmost leaf page
  // KeyType key{};  // not used
  // Page *leaf_page = FindLeafPage(key, false, nullptr, Operation::FIND, nullptr, true);  // pin leftmost leaf page
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, false, true).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_node->GetSize());  // 注意：此时leaf_node没有unpin
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  return FindLeafPageByOperation(key, Operation::FIND, nullptr, leftMost, false).first;
}

INDEX_TEMPLATE_ARGUMENTS
std::pair<Page *, bool> BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation,
                                                                Transaction *transaction, bool leftMost,
                                                                bool rightMost) {
  // 1. 下降，找到叶节点
  // 2. 叶节点kv数组二分找到k等于key

  assert(operation == Operation::FIND ? !(leftMost && rightMost) : transaction != nullptr);

  root_latch_.lock();
  bool is_root_page_id_latched = true;

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (operation == Operation::FIND) {
    page->RLatch();
    is_root_page_id_latched = false;
    root_latch_.unlock();
  } else {
    page->WLatch();
    if (IsSafe(node, operation)) {
      is_root_page_id_latched = false;
      root_latch_.unlock();
    }
  }

  while (!node->IsLeafPage()) {
    InternalPage *i_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_node_page_id;
    if (leftMost) {
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);
    }

    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (operation == Operation::FIND) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);
      // child node is safe, release all locks on ancestors
      if (IsSafe(child_node, operation)) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          root_latch_.unlock();
        }
        UnlockUnpinPages(transaction);
      }
    }

    page = child_page;
    node = child_node;
  }  // end while

  return std::make_pair(page, is_root_page_id_latched);
}

/* unlock all pages */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  // unlock 和 unpin 事务经过的所有parent page
  for (Page *page : *transaction->GetPageSet()) {  
    page->WUnlatch();
  }
  transaction->GetPageSet()->clear();  // 清空page set

}

/* unlock and unpin all pages */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {

  if (transaction == nullptr) {
    return;
  }

  for (Page *page : *transaction->GetPageSet()) { 

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  
  }
  transaction->GetPageSet()->clear();  
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N *node, Operation op) {
  if (node->IsRootPage()) {
    return (op == Operation::INSERT && node->GetSize() < node->GetMaxSize() - 1) ||
           (op == Operation::DELETE && node->GetSize() > 2);
  }

  if (op == Operation::INSERT) {
    return node->GetSize() < node->GetMaxSize() - 1;
  }

  if (op == Operation::DELETE) {
    return node->GetSize() > node->GetMinSize();
  }


  return true;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
