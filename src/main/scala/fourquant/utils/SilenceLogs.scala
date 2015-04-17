package fourquant.utils

import org.apache.log4j.Logger
import org.apache.log4j.varia.NullAppender

/**
 * Turn the logging completely off since it is so annoying for debugging
 * Created by mader on 4/14/15.
 */
trait SilenceLogs {
  Logger.getRootLogger().removeAllAppenders()
  Logger.getRootLogger().addAppender(new NullAppender())
}
