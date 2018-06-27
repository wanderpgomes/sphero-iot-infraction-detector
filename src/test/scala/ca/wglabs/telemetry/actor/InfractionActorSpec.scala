package ca.wglabs.telemetry.actor

import java.util.Date

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import ca.wglabs.telemetry.model._
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

class InfractionActorSpec extends TestKit(ActorSystem())
  with ImplicitSender
  with WordSpecLike
  with BeforeAndAfterAll
  with MustMatchers {


  val actorUnderTest = TestActorRef[InfractionActor]


  "An Infraction Actor" should {
    "add a new device when it receives a 'DeviceJoined' message" in {
      val probe = TestProbe()
      val deviceName = "new-device"
      actorUnderTest ! DeviceJoined(deviceName, probe.ref)
      actorUnderTest.underlyingActor.devices.size mustEqual 1
      actorUnderTest.underlyingActor.devices.head._1 mustEqual deviceName
    }

    "remove a device when it receives a 'DeviceLeft' message" in {
      val probe = TestProbe()
      val deviceName = "new-device"
      actorUnderTest ! DeviceJoined(deviceName, probe.ref)
      actorUnderTest ! DeviceLeft(deviceName)
      actorUnderTest.underlyingActor.devices.size mustEqual 0
    }

    "increment the device's score for the device associated with a 'VelocityInfraction' message" in {
      val probe = TestProbe()
      val deviceName = "new-device"
      actorUnderTest ! DeviceJoined(deviceName, probe.ref)
      actorUnderTest ! VelocityInfractionDetected(deviceName, Velocity("mm/s", 900, 900, 1272.0), Position("mm", 100, 100), new Date())
      actorUnderTest.underlyingActor.devices.head._2.device.points mustEqual 1
    }

    "not increment the device's score for the device associated with a 'VelocityInfraction' message if it is exempt" in {
      val probe = TestProbe()
      val deviceName = "new-device"
      actorUnderTest ! DeviceJoined(deviceName, probe.ref)
      actorUnderTest ! VelocityInfractionDetected(deviceName, Velocity("mm/s", 900, 900, 1272.0), Position("mm", 100, 100), new Date())
      actorUnderTest.underlyingActor.devices.head._2.device.points mustEqual 1
      actorUnderTest ! VelocityInfractionDetected(deviceName, Velocity("mm/s", 900, 900, 1272.0), Position("mm", 100, 100), new Date())
      actorUnderTest.underlyingActor.devices.head._2.device.points mustEqual 1
    }

    "send an 'InfractionExempt' message to itself after receiving a 'VelocityInfraction' message" in {
      val probe = TestProbe()
      val deviceName = "new-device"
      actorUnderTest ! DeviceJoined(deviceName, probe.ref)
      actorUnderTest ! VelocityInfractionDetected(deviceName, Velocity("mm/s", 900, 900, 1272.0), Position("mm", 100, 100), new Date())
      actorUnderTest.underlyingActor.devices.head._2.device.isExempt mustEqual true
    }

    "send a 'DeviceCommand(yellow)' message to the device actor after receiving the first 'VelocityInfraction' message" in {
      val probe = TestProbe()
      val deviceName = "new-device"
      actorUnderTest ! DeviceJoined(deviceName, probe.ref)
      actorUnderTest ! VelocityInfractionDetected(deviceName, Velocity("mm/s", 900, 900, 1272.0), Position("mm", 100, 100), new Date())
      probe.expectMsg(DeviceResponse("yellow"))
    }

    "send a 'DeviceCommand(red)' message to the device actor after receiving a second 'VelocityInfraction' message and it's not exempt" in {
      val probe = TestProbe()
      val deviceName = "new-device"
      actorUnderTest ! DeviceJoined(deviceName, probe.ref)
      actorUnderTest ! VelocityInfractionDetected(deviceName, Velocity("mm/s", 900, 900, 1272.0), Position("mm", 100, 100), new Date())
      probe.expectMsg(DeviceResponse("yellow"))
      actorUnderTest ! InfractionExempted(deviceName, false)
      actorUnderTest ! VelocityInfractionDetected(deviceName, Velocity("mm/s", 900, 900, 1272.0), Position("mm", 100, 100), new Date())
      probe.expectMsg(DeviceResponse("red"))
    }
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
