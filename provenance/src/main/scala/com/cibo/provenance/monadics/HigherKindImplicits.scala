package com.cibo.provenance.monadics

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


trait Applicable[T[_]] extends Serializable {
  def apply[A](idx: Int)(lok: T[A]): A
  def indices[A](ta: T[A]): Range
}

trait Mappable[T[_]] extends Serializable {
  def map[A, B](f: A => B)(lok: T[A]): T[B]
}

trait Traversable[T[_]] extends Applicable[T] with Mappable[T] with Serializable {
  def toSeq[A](lok: T[A]): Seq[A]
  def toList[A](lok: T[A]): List[A]
  def toVector[A](lok: T[A]): Vector[A]
}

object Mappable {
  implicit object OptionMethods extends Mappable[Option] with Serializable {
    def map[A, B](f: A => B)(lok: Option[A]): Option[B] = lok.map(f)
    def toSeq[A](lok: Option[A]): Seq[A] = lok.toSeq
    def toList[A](lok: Option[A]): List[A] = lok.toList
    def toVector[A](lok: Option[A]): Vector[A] = lok.toVector
    //def toArray[A](lok: Option[A]): Array[A] = lok.toArray
  }
}

object Traversable {
  implicit object SeqMethods extends Traversable[Seq] with Serializable {
    def map[A, B](f: A => B)(lok: Seq[A]): Seq[B] = lok.map(f)
    def apply[A](idx: Int)(lok: Seq[A]): A = lok.apply(idx)
    def indices[A](lok: Seq[A]): Range = lok.indices
    def toSeq[A](lok: Seq[A]): Seq[A] = lok
    def toList[A](lok: Seq[A]): List[A] = lok.toList
    def toVector[A](lok: Seq[A]): Vector[A] = lok.toVector
    //def toArray[A](lok: Seq[A]): Array[A] = lok.toArray
  }
  implicit object ListMethods extends Traversable[List] with Serializable {
    def map[A, B](f: A => B)(lok: List[A]): List[B] = lok.map(f)
    def apply[A](idx: Int)(lok: List[A]): A = lok.apply(idx)
    def indices[A](lok: List[A]): Range = lok.indices
    def toSeq[A](lok: List[A]): Seq[A] = lok
    def toList[A](lok: List[A]): List[A] = lok
    def toVector[A](lok: List[A]): Vector[A] = lok.toVector
    //def toArray[A](lok: List[A]): Array[A] = lok.toArray
  }
  implicit object VectorMethods extends Traversable[Vector] with Serializable {
    def map[A, B](f: A => B)(lok: Vector[A]): Vector[B] = lok.map(f)
    def apply[A](idx: Int)(lok: Vector[A]): A = lok.apply(idx)
    def indices[A](lok: Vector[A]): Range = lok.indices
    def toSeq[A](lok: Vector[A]): Seq[A] = lok
    def toList[A](lok: Vector[A]): List[A] = lok.toList
    def toVector[A](lok: Vector[A]): Vector[A] = lok
    //def toArray[A](lok: Vector[A]): Array[A] = lok.toArray
  }

  /*
  implicit object MappableArray extends Mappable[Array] with Applicable[Array] {
    def map[A, B](f: A => B)(va: Array[A]): Array[B] = va.map(f)
    def apply[A](idx: Int)(va: Array[A]): A = va.apply(idx)
    def indices[A](lok: Array[A]): Range = lok.indices
    def toSeq[A](lok: Array[A]): Seq[A] = lok.toSeq
    def toList[A](lok: Array[A]): List[A] = lok.toList
    def toVector[A](lok: Array[A]): Vector[A] = lok.toVector
    //def toArray[A](lok: Array[A]): Array[A] = lok
  }
  */
}
