package com.socgen.bsc.cbs.businessviews.common

import org.slf4j.{Logger, LoggerFactory}

/**
  * Utility trait for classes that want to use logging services without spark serialisation issues.
  */
trait LoggingSupport {

  @transient private lazy val logger : Logger = LoggerFactory.getLogger(loggerName)

  private def loggerName = this.getClass.getName.stripSuffix("$")

  protected def info(msg: => String) = if (logger.isInfoEnabled) logger.info(msg)

  protected def info(msg: => String, throwable: Throwable) = if (logger.isInfoEnabled) logger.info(msg, throwable)

  protected def debug(msg: => String) =if (logger.isDebugEnabled) logger.debug(msg)

  protected def debug(msg: => String, throwable: Throwable) = if (logger.isDebugEnabled) logger.debug(msg, throwable)

  protected def warning(msg: => String) = if (logger.isWarnEnabled) logger.warn(msg)

  protected def warning(msg: => String, throwable: Throwable) = if (logger.isWarnEnabled) logger.warn(msg, throwable)

  protected def error(msg: => String) = if (logger.isErrorEnabled) logger.error(msg)

  protected def error(msg: => String, throwable: Throwable) = if (logger.isErrorEnabled) logger.error(msg, throwable)
}