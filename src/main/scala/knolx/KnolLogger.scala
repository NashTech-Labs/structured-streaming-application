package knolx

import org.apache.log4j.{Level, LogManager}

/**
  * Copyright Knoldus, Inc. 2018. All rights reserved.
  */
trait KnolLogger {
  private val logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  def debug(message: String): Unit = logger.debug(message)

  def info(message: String): Unit = logger.info(message)

  def warn(message: String): Unit = logger.warn(message)

  def error(message: String, throwable: Throwable): Unit = logger.error(message, throwable)
}
