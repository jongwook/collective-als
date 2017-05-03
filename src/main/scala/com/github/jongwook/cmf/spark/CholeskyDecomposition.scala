package com.github.jongwook.cmf.spark

import com.github.fommil.netlib.LAPACK.{ getInstance => lapack }
import org.netlib.util.intW

object CholeskyDecomposition {

  /**
   * Solves a symmetric positive definite linear system via Cholesky factorization.
   * The input arguments are modified in-place to store the factorization and the solution.
   *
   * @param A the upper triangular part of A
   * @param bx right-hand side
   * @return the solution array
   */
  def solve(A: Array[Double], bx: Array[Double]): Array[Double] = {
    val k = bx.length
    val info = new intW(0)
    lapack.dppsv("U", k, 1, A, bx, k, info)
    val code = info.`val`
    assert(code == 0, s"lapack.dppsv returned $code.")
    bx
  }

  /**
   * Computes the inverse of a real symmetric positive definite matrix A
   * using the Cholesky factorization A = U**T*U.
   * The input arguments are modified in-place to store the inverse matrix.
   *
   * @param UAi the upper triangular factor U from the Cholesky factorization A = U**T*U
   * @param k the dimension of A
   * @return the upper triangle of the (symmetric) inverse of A
   */
  def inverse(UAi: Array[Double], k: Int): Array[Double] = {
    val info = new intW(0)
    lapack.dpptri("U", k, UAi, info)
    val code = info.`val`
    assert(code == 0, s"lapack.dpptri returned $code.")
    UAi
  }
}
