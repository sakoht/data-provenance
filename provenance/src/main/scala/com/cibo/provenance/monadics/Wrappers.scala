package com.cibo.provenance.monadics

/**
  * Created by ssmith on 11/10/17.
  *
  * Traits representing the ability to do .apply, .map, .indices, etc. on higher-order kinds.
  *
  * Each monadic works with an S[A] where S is th kind (Seq, Vector, Option),
  * and takes an implicit value like `hok: Traversable[S]` to access the higher-order "kinds".
  * 
  * The hok allows access to an appropriate .map, .apply, .indices, etc.
  * 
  * This is a tiny slice of concepts in "cats".
  * Switch to that if this begins to balloon.
  */

import scala.language.higherKinds

object Wrappers {
  import com.cibo.provenance.monadics.Mappable._
  import com.cibo.provenance.monadics.Traversable._
}

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
  implicit object OptionWrapper extends Mappable[Option] with Serializable {
    def map[A, B](f: A => B)(lok: Option[A]): Option[B] = lok.map(f)
    def toSeq[A](lok: Option[A]): Seq[A] = lok.toSeq
    def toList[A](lok: Option[A]): List[A] = lok.toList
    def toVector[A](lok: Option[A]): Vector[A] = lok.toVector
    //def toArray[A](lok: Option[A]): Array[A] = lok.toArray
  }
}

object Traversable {
  implicit object SeqWrapper extends Traversable[Seq] with Serializable {
    def map[A, B](f: A => B)(lok: Seq[A]): Seq[B] = lok.map(f)
    def apply[A](idx: Int)(lok: Seq[A]): A = lok.apply(idx)
    def indices[A](lok: Seq[A]): Range = lok.indices
    def toSeq[A](lok: Seq[A]): Seq[A] = lok
    def toList[A](lok: Seq[A]): List[A] = lok.toList
    def toVector[A](lok: Seq[A]): Vector[A] = lok.toVector
    //def toArray[A](lok: Seq[A]): Array[A] = lok.toArray
  }
  implicit object ListWrapper extends Traversable[List] with Serializable {
    def map[A, B](f: A => B)(lok: List[A]): List[B] = lok.map(f)
    def apply[A](idx: Int)(lok: List[A]): A = lok.apply(idx)
    def indices[A](lok: List[A]): Range = lok.indices
    def toSeq[A](lok: List[A]): Seq[A] = lok
    def toList[A](lok: List[A]): List[A] = lok
    def toVector[A](lok: List[A]): Vector[A] = lok.toVector
    //def toArray[A](lok: List[A]): Array[A] = lok.toArray
  }
  implicit object VectorWrapper extends Traversable[Vector] with Serializable {
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
