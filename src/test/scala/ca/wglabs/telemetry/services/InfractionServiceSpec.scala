package ca.wglabs.telemetry.services

import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import ca.wglabs.telemetry.model.{DeviceMeasurement, Measurement, Position, Velocity}
import org.scalatest.{FunSuite, Matchers, MustMatchers}

class InfractionServiceSpec extends FunSuite with MustMatchers with ScalatestRouteTest {

    val deviceInfractionService = new InfractionService

    test("Connect to web socket") {
      val wsClient = WSProbe()
      WS("/measurements/bb8", wsClient.flow) ~> deviceInfractionService.route ~>
        check {
          // check response for WS Upgrade headers
          isWebSocketUpgrade mustBe true
        }
    }

    test("Send command yellow back if velocity is greater than the maximum of 900") {
      val wsClient = WSProbe()
      val sensorData = """{
        | "velocity": { "unit": "mm/s", "v": 0.0, "vx": 100, "vy": 900 },
        | "position": { "unit": "cm", "x": 10, "y": 15 }
        |}
      """.stripMargin

      WS("/measurements/bb8", wsClient.flow) ~> deviceInfractionService.route ~>
      check {
        wsClient.sendMessage(sensorData)
        wsClient.expectMessage("{\"color\":\"yellow\"}")
      }
    }

  test("Send command yellow back if velocity is greater than the maximum of 900 twice within 5 seconds") {
    val wsClient = WSProbe()
    val sensorData = """{
      | "velocity": { "unit": "mm/s", "v": 0.0, "vx": 100, "vy": 900 },
      | "position": { "unit": "cm", "x": 10, "y": 15 }
      |}
    """.stripMargin

    WS("/measurements/bb8", wsClient.flow) ~> deviceInfractionService.route ~>
      check {
        wsClient.sendMessage(sensorData)
        wsClient.expectMessage("{\"color\":\"yellow\"}")
      }
  }

  test("Detect Wrong Way infraction if last measurement is closer to origin") {
    val velocity = Velocity("mm/s", 900, 900)
    var measurements = (1 to 3).map(n => DeviceMeasurement("device", Measurement(velocity, Position("cm", 0 + n, 0))))
    val measurementWithInfraction = DeviceMeasurement("device", Measurement(velocity, Position("cm", 2, 0)))
    measurements = measurements :+ measurementWithInfraction

    val result = deviceInfractionService.isWrongWayInfraction(measurements)

    result mustBe true
  }

  test("Do not detect Wrong Way infraction if last measurement is further from origin") {
    val velocity = Velocity("mm/s", 900, 900)
    val measurements = (1 to 3).map(n => DeviceMeasurement("device",Measurement(velocity, Position("cm", 0 + n, 0))))

    val result = deviceInfractionService.isWrongWayInfraction(measurements)

    result mustBe false
  }

  test("Do not detect Wrong Way infraction if last measurements have same position") {
    val velocity = Velocity("mm/s", 900, 900)
    val measurements = (1 to 3).map(_ => DeviceMeasurement("device", Measurement(velocity, Position("cm", 0, 0))))

    val result = deviceInfractionService.isWrongWayInfraction(measurements)

    result mustBe false
  }

  test("Do not detect Wrong Way infraction if there is only one measurement") {
    val velocity = Velocity("mm/s", 900, 900)
    val measurements = List(DeviceMeasurement("device", Measurement(velocity, Position("cm", 0 , 0))))

    val result = deviceInfractionService.isWrongWayInfraction(measurements)

    result mustBe false
  }

}

