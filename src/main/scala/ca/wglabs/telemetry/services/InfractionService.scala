package ca.wglabs.telemetry.services

import java.lang.Math._
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import ca.wglabs.telemetry.model._
import ca.wglabs.telemetry.services.constants._
import ca.wglabs.telemetry.util.JsonFormat._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import spray.json._

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try



class InfractionService(implicit val system : ActorSystem, implicit val actorMaterializer: ActorMaterializer) extends Directives {

  final case class EventMessage[+T](body: T, originalMessage: CommittableMessage[String, Array[Byte]])



  val consumerConfig = system.settings.config.getConfig("akka.kafka.consumer")

  val consumerSettings =
    ConsumerSettings(consumerConfig, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def consumerGraph[A] =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics("test"))
      .via(parseRawMessageFlow[A])
      .mapAsync(5)(_.originalMessage.committableOffset.commitScaladsl())
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)


  def business(key: String, value: Array[Byte]): Future[Done] = {
    val str = new String(value, StandardCharsets.UTF_8)

    print(s"Akka stream consuming: $str")
    Future.successful(Done)
  }

  def parseRawMessageFlow[A] : Flow[CommittableMessage[String, Array[Byte]], EventMessage[A], NotUsed] = {
    Flow[CommittableMessage[String, Array[Byte]]]
      .map(convertEventMessage)
      .map(parseEventMessage[A])
      .filter(msg => msg.body.isRight)
      .map(msg => msg.copy(body = msg.body.right.get))
  }


  def convertEventMessage(message: CommittableMessage[String, Array[Byte]]): EventMessage[String] =
    EventMessage(new String(message.record.value, StandardCharsets.UTF_8), message)



  def parseEventMessage[A: JsonFormat](input: EventMessage[String]): EventMessage[Either[Throwable, A]] = {
    val errorOrDeviceMeasurement = Try(jsonReader.read(input.body.parseJson)).toEither
    input.copy(body = errorOrDeviceMeasurement)
  }

  def detectVelocityInfractionFlow: Flow[DeviceMeasurement, VelocityInfractionDetected, NotUsed] =
    Flow[DeviceMeasurement]
      .map(calculateVelocity)
      .filter(isVelocityInfraction)
      .map(m => VelocityInfractionDetected(m.name, m.measurement.velocity, m.measurement.position, new Date()))

  def detectWrongWayInfractionFlow: Flow[DeviceMeasurement, WrongWayInfractionDetected, NotUsed] =
    Flow[DeviceMeasurement]
      .groupedWithin(10, 2 seconds)
      .filter(isWrongWayInfraction)
      .map(ms => WrongWayInfractionDetected(ms.head.name, ms.head.measurement.position, LocalDateTime.now()))



  def isVelocityInfraction(m: DeviceMeasurement) = {
    m.measurement.velocity.v > velocityLimit
  }

  def isWrongWayInfraction(measurements: Seq[DeviceMeasurement]) = {
    calculateDistanceDifference(measurements.toList, 0.0) < 0.0
  }

  def calculateVelocity(m: DeviceMeasurement): DeviceMeasurement = {
    val velocity = Try(sqrt(pow(m.measurement.velocity.vx, 2) + pow(m.measurement.velocity.vy, 2))).getOrElse(0.0)
    m.measurement.velocity.v = velocity; m
  }

  @tailrec
  private def calculateDistanceDifference(measurements: List[DeviceMeasurement], acc: Double): Double = measurements match {
    case Nil => acc
    case _ :: Nil => acc
    case m :: ms =>
      if (acc >= 0.0) calculateDistanceDifference(ms.tail, distanceToOrigin(ms.head) - distanceToOrigin(m))
      else acc
  }

  private def distanceToOrigin(dm: DeviceMeasurement) = {
    Try(sqrt(pow(dm.measurement.position.x, 2) + pow(dm.measurement.position.y, 2))).getOrElse(0.0)
  }
}

