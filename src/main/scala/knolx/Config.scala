package knolx

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */
object Config {
  val bootstrapServer = System.getenv("BOOTSTRAP_SERVERS_CONFIG")
  val topic = System.getenv("TOPIC")
}
