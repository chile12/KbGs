package org.aksw.kbgs.inout

import java.io.Reader

/**
 * Created by Chile on 8/29/2015.
 */
trait WorkLoader[+W] extends Reader with Iterator[Option[W]]  {
}
