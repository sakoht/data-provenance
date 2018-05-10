package com.cibo.provenance.implicits

/**
  * Created by ssmith on 11/10/17 with significant help from @tel.
  *
  * This is a tiny slice of concepts in "cats".  Switch to that if this begins to balloon.
  *
  * These are traits and implicit objects for "higher-kinds", for instance, with
  * higher kind S[_], S might be `Seq` or `Option` or `Future`.
  *
  * For example are several `Traversable[S]` implicit objects:
  * - SeqMethods is a `Traversable[Seq]` object, subclassing Traversable[ S[_] ]
  * - VectorMethods is a `Traversable[Vector]` object, subclassing Traversable [ [S_] ]
  * - ...
  *
  * An implicit Traversable[Seq] is part of the signature for implicit classes that use S[_],
  * but need to be specific about what _ is.  This isn't part of the type parameters, as a naive person might expect.
  * Its presence in signature of an implicit class controls where the implicit class is applied.
  *
  * For instance, in the object FunctionCallWithProvenance[O], we add methods like map
  * through an implicit class that recognizes O is S[_], there is a Traversable[S] implicitly available.
  *
  *   implicit class TraversableCall[S[_], A](call: FunctionCallWithProvenance[S[A]])(implicit hok: Traversable[S],..)
  *
  * The hok allows access to an appropriate .map, .apply, .indices, etc., even though S[A] is otherwise an an
  * anonymous structure in the code.
  *
  */

import scala.language.higherKinds
import scala.reflect.ClassTag

trait Traversable[S[_]] extends Serializable {
  def map[A, B](f: A => B)(lok: S[A]): S[B]
  def apply[A](idx: Int)(lok: S[A]): A
  def indicesRange[A](ta: S[A]): Range
  def indicesTraversable[A](lok: S[A]): S[Int]
  def toSeq[A](lok: S[A]): Seq[A]
  def toList[A](lok: S[A]): List[A]
  def toVector[A](lok: S[A]): Vector[A]
  def outerClassTag[A : ClassTag]: ClassTag[_]
  def innerClassTag[A : ClassTag]: ClassTag[_]
}

object Traversable {

  implicit object SeqMethods extends Traversable[Seq] with Serializable {
    def map[A, B](f: A => B)(lok: Seq[A]): Seq[B] = lok.map(f)
    def apply[A](idx: Int)(lok: Seq[A]): A = lok.apply(idx)
    def indicesRange[A](lok: Seq[A]): Range = lok.indices
    def indicesTraversable[A](lok: Seq[A]): Seq[Int] = lok.indices.toList // don't use .toSeq it will keep Range
    def toSeq[A](lok: Seq[A]): Seq[A] = lok
    def toList[A](lok: Seq[A]): List[A] = lok.toList
    def toVector[A](lok: Seq[A]): Vector[A] = lok.toVector
    def outerClassTag[A : ClassTag]: ClassTag[Seq[A]] = implicitly[ClassTag[Seq[A]]]
    def innerClassTag[A : ClassTag]: ClassTag[A] = implicitly[ClassTag[A]]
  }

  implicit object ListMethods extends Traversable[List] with Serializable {
    def map[A, B](f: A => B)(lok: List[A]): List[B] = lok.map(f)
    def apply[A](idx: Int)(lok: List[A]): A = lok.apply(idx)
    def indicesRange[A](lok: List[A]): Range = lok.indices
    def indicesTraversable[A](lok: List[A]): List[Int] = lok.indices.toList
    def toSeq[A](lok: List[A]): Seq[A] = lok
    def toList[A](lok: List[A]): List[A] = lok
    def toVector[A](lok: List[A]): Vector[A] = lok.toVector
    def outerClassTag[A : ClassTag]: ClassTag[List[A]] = implicitly[ClassTag[List[A]]]
    def innerClassTag[A : ClassTag]: ClassTag[A] = implicitly[ClassTag[A]]
  }

  implicit object VectorMethods extends Traversable[Vector] with Serializable {
    def map[A, B](f: A => B)(lok: Vector[A]): Vector[B] = lok.map(f)
    def apply[A](idx: Int)(lok: Vector[A]): A = lok.apply(idx)
    def indicesRange[A](lok: Vector[A]): Range = lok.indices
    def indicesTraversable[A](lok: Vector[A]): Vector[Int] = lok.indices.toVector
    def toSeq[A](lok: Vector[A]): Seq[A] = lok
    def toList[A](lok: Vector[A]): List[A] = lok.toList
    def toVector[A](lok: Vector[A]): Vector[A] = lok
    def outerClassTag[A : ClassTag]: ClassTag[Vector[A]] = implicitly[ClassTag[Vector[A]]]
    def innerClassTag[A : ClassTag]: ClassTag[A] = implicitly[ClassTag[A]]
  }
}
