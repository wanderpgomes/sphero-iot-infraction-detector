package ca.wglabs.telemetry.model

import java.time.LocalDateTime
import java.util.Date

import akka.actor.ActorRef


sealed trait DeviceEvent

case class DeviceLeft(name: String) extends DeviceEvent
case class DeviceJoined(name: String, actorRef: ActorRef) extends DeviceEvent

case class DeviceResponse(color: String) extends DeviceEvent

trait Infraction {
  val deviceName : String
}
case class VelocityInfractionDetected(deviceName: String, velocity: Velocity, position: Position, date: Date) extends DeviceEvent with Infraction
case class WrongWayInfractionDetected(deviceName: String, position: Position, date: LocalDateTime) extends DeviceEvent with Infraction
case class InfractionExempted(name: String, exempt: Boolean) extends DeviceEvent

case class DeviceMeasurement(name: String, measurement: Measurement)
case class Measurement(velocity: Velocity, position: Position)
case class Velocity(unit: String, vx: Int, vy: Int, var v: Double = 0.0)
case class Position(unit: String, x: Int, y: Int)
