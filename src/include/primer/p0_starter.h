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

#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
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
    if (linear_) {
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
  /// @note dependent and non-dependent name.
  /// @ref https://isocpp.org/wiki/faq/templates#nondependent-name-lookup-members
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    // allocate
    data_ = new T *[this->rows_];
    assert(data_);

    // assign
    for (int i = 0; i < this->rows_; ++i) {
      data_[i] = &this->linear_[i * this->cols_];
    }
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
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "invalid index");
    }
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
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "invalid index");
    }
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
    if (static_cast<int>(source.size()) != dst_size) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "incorrect source size");
    }

    assert(this->linear_);
    for (int i = 0; i < dst_size; ++i) {
      this->linear_[i] = source.at(i);
    }
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
   * Compute (`matrix_a` + `matrix_b`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrix_a Input matrix
   * @param matrix_b Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrix_a, const RowMatrix<T> *matrix_b) {
    // TODO(P0): Add implementation

    if (!matrix_a || !matrix_b) {
      return {nullptr};
    }

    // check if the inputs are compatible.
    const int rows_a = matrix_a->GetRowCount();
    const int cols_a = matrix_a->GetColumnCount();
    const int rows_b = matrix_b->GetRowCount();
    const int cols_b = matrix_b->GetColumnCount();
    if (rows_a != rows_b || cols_a != cols_b) {
      return {nullptr};
    }

    auto mat = std::make_unique<RowMatrix<T>>(rows_a, cols_a);
    for (int i = 0; i < rows_a; ++i) {
      for (int j = 0; j < cols_a; ++j) {
        const T val = matrix_a->GetElement(i, j) + matrix_b->GetElement(i, j);
        mat->SetElement(i, j, val);
      }
    }

    return mat;
  }

  /**
   * Compute the matrix multiplication (`matrix_a` * `matrix_b` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrix_a Input matrix
   * @param matrix_b Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrix_a, const RowMatrix<T> *matrix_b) {
    // TODO(P0): Add implementation

    if (!matrix_a || !matrix_b) {
      return {nullptr};
    }

    // check if the inputs are compatible.
    const int rows_a = matrix_a->GetRowCount();
    const int cols_a = matrix_a->GetColumnCount();
    const int rows_b = matrix_b->GetRowCount();
    const int cols_b = matrix_b->GetColumnCount();
    if (cols_a != rows_b) {
      return {nullptr};
    }

    auto mat = std::make_unique<RowMatrix<T>>(rows_a, cols_b);
    for (int i = 0; i < rows_a; ++i) {
      for (int j = 0; j < cols_b; ++j) {
        int sum = 0;
        for (int k = 0; k < rows_b; ++k) {
          sum += matrix_a->GetElement(i, k) * matrix_b->GetElement(k, j);
        }
        mat->SetElement(i, j, sum);
      }
    }

    return mat;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrix_a` * `matrix_b` + `matrix_c`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrix_a Input matrix
   * @param matrix_b Input matrix
   * @param matrix_c Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrix_a, const RowMatrix<T> *matrix_b,
                                            const RowMatrix<T> *matrix_c) {
    // TODO(P0): Add implementation

    auto mat = Multiply(matrix_a, matrix_b);
    if (!mat) {
      return {nullptr};
    }

    mat = Add(mat.get(), matrix_c);
    if (!mat) {
      return {nullptr};
    }

    return mat;
  }
};
}  // namespace bustub
