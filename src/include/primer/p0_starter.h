//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// niebayes 2021-11-01
// niebayes@gmail.com

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) : rows_{rows}, cols_{cols} {
    linear_ = new T[rows_ * cols_];
    assert(linear_);
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() {
    if (not linear_) {
      delete[] linear_;
      linear_ = nullptr;
    }
  }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  ///@note dependent and non-dependent name.
  ///@ref https://isocpp.org/wiki/faq/templates#nondependent-name-lookup-members
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {

    // allocate
    data_ = new T*[this->rows_];
    assert(data_);

    // assign
    for (int i = 0; i < this->rows_; ++i) 
      data_[i] = &this->linear_[i * this->cols_];
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) 
      throw std::out_of_range("invalid index");
    return data_[i][j];
  }

  /** 
   * 
   * TODO(P0): Add implementation
   * 
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) 
      throw std::out_of_range("invalid index");
    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    const int dst_size = this->rows_ * this->cols_;
    if (static_cast<int>(source.size()) != dst_size) 
      throw std::out_of_range("incorrect source size");
    
    assert(this->linear_);
    for (int i = 0; i < dst_size; ++i)
      this->linear_[i] = source.at(i);
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    if (data_) {
      delete[] data_;
      data_ = nullptr;
    }
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation

    // check if the inputs are compatible.
    const int rows_A = matrixA->GetRowCount(), cols_A = matrixA->GetColumnCount();
    const int rows_B = matrixB->GetRowCount(), cols_B = matrixB->GetColumnCount();
    if (rows_A != rows_B || cols_A != cols_B) 
      return {nullptr};

    auto mat = std::make_unique<RowMatrix<T>>(rows_A, cols_A);
    for (int i = 0; i < rows_A; ++i) {
      for (int j = 0; j < cols_A; ++j) {
        const T val = matrixA->GetElement(i, j) + matrixB->GetElement(i, j);
        mat->SetElement(i, j, val);
      }
    }

    return mat;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation

    // check if the inputs are compatible.
    const int rows_A = matrixA->GetRowCount(), cols_A = matrixA->GetColumnCount();
    const int rows_B = matrixB->GetRowCount(), cols_B = matrixB->GetColumnCount();
    if (cols_A != rows_B) 
      return {nullptr};

    auto mat = std::make_unique<RowMatrix<T>>(rows_A, cols_B);
    for (int i = 0; i < rows_A; ++i) {
      for (int j = 0; j < cols_B; ++j) {
        int sum = 0;
        for (int k = 0; k < rows_B; ++k) 
          sum += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
        mat->SetElement(i, j, sum);
      }
    }

    return mat;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    
    auto mat = Multiply(matrixA, matrixB);
    if (not mat) return {nullptr};

    mat = Add(mat->get(), matrixC);
    if (not mat) return {nullptr};

    return mat;
  }
};
}  // namespace bustub
