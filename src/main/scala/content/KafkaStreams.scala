package content

/**
 * case class are automatically serializable to json through circe package which is based on macro.
 * It will create encoders and decoders for case classes
 */
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties // this will be able to convert a serde we create


object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String // case class in a real life
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, userId: UserId, products: List[Product], amount: BigDecimal)
    case class Discount(profile: Profile, amount: BigDecimal)
    case class Payment(orderId: OrderId, status: Status)
  }

   object Topics {
    val OrdersByUser = "orders-by-user"
    val DiscountProfilesByUser = "discount-profile-by-user"
    val Discounts = "discounts"
    val Orders = "orders"
    val Payments = "payments"
    val PaidOrders = "paid-orders"
  }

  /**
    List(
    "orders-by-user",
    "discount-profile-by-user",
    "discounts",
    "orders",
    "payments",
    "paid-orders"
    ).foreach{
      topic => println(s"kafka-topics --bootstrap-server localhost:9092 --topic $topic --create")
    }*/

    import Domain._
    import Topics._
    implicit def serde[A >: Null: Decoder: Encoder]: Serde[A] =  {
      val serializer = (a: A) => a.asJson.noSpaces.getBytes()
      val deserializer = (bytes: Array[Byte]) => {
        val string = new String(bytes)
        decode[A](string).toOption
      }
      Serdes.fromFn[A](serializer, deserializer)
    }

  def main(args: Array[String]): Unit = {
    // source = emits elements
    // flow = transform elements along the way (e.g map)
    // sink = "ingests" elements


    val builder = new StreamsBuilder()

    // KStream : describe a linear flow
    val usersOrdersStream: KStream[UserId, Order] = builder.stream[UserId,Order](OrdersByUser)

    // KTable (distributed to the kafka nodes): the topic(DiscountProfilesByUser) need to be created with compact configuration
    // kafka-topics --bootstrap-server localhost:9092 --topic discount-profile-by-user --create --config "cleanup.policy=compact"
    val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUser)

    // GlobalKTable (distributed to all the nodes in the kafka cluster): should store few values; topic need to be compat too
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Discounts)

    // KStream transformation: filter, map, mapValues, flatMap, flatMapValues
    val expensiveOrders = usersOrdersStream.filter { (userId, order) =>
      order.amount > 1000
    }
    val listsOfProducts = usersOrdersStream.mapValues { order =>
      order.products
    }

    val productsStream = usersOrdersStream.flatMapValues(_.products)

    // Join
    val ordersWithUserProfiles = usersOrdersStream.join(userProfilesTable){ (order,profile) =>
      (order, profile)
    }

    val discountedOrdersStream = ordersWithUserProfiles.join(discountProfilesGTable)(
      {case (userId, (order, profile)) => profile}, // key of the join - picked from the "left" stream
      {case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount)} // value of the matched records
    )

    // pick another identifier
    val ordersStream = discountedOrdersStream.selectKey((userId, order) => order.orderId)
    val paymentsStream = builder.stream[OrderId, Payment](Payments)

    val aJoinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

    // Option.empty[T] means something is None for now but can become Some(x) in the future.
    val joinOrdersPayments = (order: Order, payment: Payment) => if (payment.status == "PAID") Option(order) else Option.empty[Order]

    // need to give the join window because we're joining two streams
    val ordersPaid = ordersStream.join(paymentsStream)(joinOrdersPayments,aJoinWindow)
      .flatMapValues(maybeOrder => maybeOrder.toList)

    // sink
    ordersPaid.to(PaidOrders)

    // topology: describe the data flow
    val topology = builder.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    val application = new KafkaStreams(topology, props)
    application.start()
    //println(topology.describe())

  }
}
