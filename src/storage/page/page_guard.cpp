#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept : bpm_(that.bpm_), page_(that.page_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr || page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }

  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (bpm_ == nullptr || page_ == nullptr) {
    return {};
  }

  page_->RLatch();
  auto guard = ReadPageGuard{bpm_, page_};
  bpm_ = nullptr;
  page_ = nullptr;

  return guard;
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (bpm_ == nullptr || page_ == nullptr) {
    return {};
  }

  page_->WLatch();
  auto guard = WritePageGuard{bpm_, page_};
  bpm_ = nullptr;
  page_ = nullptr;

  return guard;
}
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }

  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }

  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
