package com.cibo.provenance.monaidcs

/**
  * Created by ssmith on 11/10/17.
  *
  * Traits representing the ability to do .apply, .map, .indices, etc.,
  * and impliciit
  *
  */

import scala.language.higherKinds

trait Applicable[T[_]] {
  def apply[A](idx: Int)(ta: T[A]): A
  def indices[A](ta: T[A]): T[Int]
}

trait Mappable[T[_]] {
  def map[A, B](f: A => B)(ta: T[A]): T[B]
}

trait Traversable[T[_]] extends Applicable[T] with Mappable[T]

object Traversable {
  implicit object MappableSeq extends Traversable[Seq] with Applicable[Seq] {
    def map[A, B](f: A => B)(va: Seq[A]): Seq[B] = va.map(f)
    def apply[A](idx: Int)(va: Seq[A]): A = va.apply(idx)
    def indices[A](va: Seq[A]): Seq[Int] = va.indices.toList // note: toSeq will leave this a Range which won't serialize
  }
  implicit object MappableList extends Traversable[List] with Applicable[List] {
    def map[A, B](f: A => B)(va: List[A]): List[B] = va.map(f)
    def apply[A](idx: Int)(va: List[A]): A = va.apply(idx)
    def indices[A](va: List[A]): List[Int] = va.indices.toList
  }
  implicit object MappableVector extends Traversable[Vector] with Applicable[Vector] {
    def map[A, B](f: A => B)(va: Vector[A]): Vector[B] = va.map(f)
    def apply[A](idx: Int)(va: Vector[A]): A = va.apply(idx)
    def indices[A](va: Vector[A]): Vector[Int] = va.indices.toVector
  }
  /*
  implicit object MappableArray extends Mappable[Array] with Applicable[Array] {
    def map[A, B](f: A => B)(va: Array[A]): Array[B] = va.map(f)
    def apply[A](idx: Int)(va: Array[A]): A = va.apply(idx)
    def indices[A](va: Array[A]): Array[Int] = va.indices.toArray
  }
  */
}
