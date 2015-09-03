package org.aksw.kbgs.helpers

import java.io.File

import org.apache.commons.lang3.SystemUtils

/**
 * Created by Chile on 9/3/2015.
 */
class  KbIntersectionPreparation(idFileKb1: String, idFileKb2: String) {

  val kb1IdOnly = new File(idFileKb1.substring(0, idFileKb1.lastIndexOf('.')) + "_idsOnly" + idFileKb1.substring(idFileKb1.lastIndexOf('.')))
  val kb2IdOnly = new File(idFileKb2.substring(0, idFileKb2.lastIndexOf('.')) + "_idsOnly" + idFileKb2.substring(idFileKb2.lastIndexOf('.')))
  val kb1Filtered = new File(idFileKb1.substring(0, idFileKb1.lastIndexOf('.')) + "_Filtered" + idFileKb1.substring(idFileKb1.lastIndexOf('.')))
  val kb2Filtered = new File(idFileKb2.substring(0, idFileKb2.lastIndexOf('.')) + "_Filtered" + idFileKb2.substring(idFileKb2.lastIndexOf('.')))
  val idIntersection = new File(idFileKb1.substring(0, idFileKb1.lastIndexOf('.')) + "_" + idFileKb2.substring(0, idFileKb2.lastIndexOf('.')) + ".intersection")
  val kb1TempFile = new File(idFileKb1.substring(0, idFileKb1.lastIndexOf('.')) + "_temp.gz")
  val kb2TempFile = new File(idFileKb2.substring(0, idFileKb2.lastIndexOf('.')) + "_temp.gz")
  val unsorted = new File(idFileKb1.substring(0, idFileKb1.lastIndexOf('.')) + "_" + idFileKb2.substring(0, idFileKb2.lastIndexOf('.')) + ".unsorted")
  val sorted = new File(idFileKb1.substring(0, idFileKb1.lastIndexOf('.')) + "_" + idFileKb2.substring(0, idFileKb2.lastIndexOf('.')) + ".sorted")

  try{
    intersect(idFileKb1, idFileKb2)
  }
  catch{
    case e: Exception => e.printStackTrace()
  }
  private def intersect(idFileKb1: String, idFileKb2: String) =
  {
    if(SystemUtils.IS_OS_UNIX) {
      if(!kb1IdOnly.exists())
        scala.sys.process.Process("cat " + idFileKb1 + " | perl -ne 's/(^.* - )(.*)/$2/g; print;' | sort > " + kb1IdOnly).!
      if(!kb2IdOnly.exists())
        scala.sys.process.Process("cat " + idFileKb2 + " | perl -ne 's/(^.* - )(.*)/$2/g; print;' | sort > " + kb2IdOnly).!
      if(!idIntersection.exists())
        scala.sys.process.Process("grep -Fx -f " + kb1IdOnly + " " + kb2IdOnly + " | uniq > " + idIntersection).!
      if(!kb1Filtered.exists())
        scala.sys.process.Process("grep -F -f " + idIntersection + " " + idFileKb1 + " | sort> " + kb1Filtered).!
      if(!kb2Filtered.exists())
        scala.sys.process.Process("grep -F -f " + idIntersection + " " + idFileKb2 + " | sort> " + kb2Filtered).!
    }
    else
      throw new Exception("Kb Id file intersection needs to be done on a unix system (for now), please see onliners.txt to do it manually")
  }

  def deleteFiles() =
  {
    if(kb1IdOnly.exists())
      scala.sys.process.Process("rm -rf " + kb1IdOnly).!
    if(kb2IdOnly.exists())
      scala.sys.process.Process("rm -rf " + kb2IdOnly).!
    if(idIntersection.exists())
      scala.sys.process.Process("rm -rf " + idIntersection).!
    if(kb1Filtered.exists())
      scala.sys.process.Process("rm -rf " + kb1Filtered).!
    if(kb2Filtered.exists())
      scala.sys.process.Process("rm -rf " + kb2Filtered).!
    if(kb1TempFile.exists())
      scala.sys.process.Process("rm -rf " + kb1TempFile).!
    if(kb2TempFile.exists())
      scala.sys.process.Process("rm -rf " + kb2TempFile).!
    if(unsorted.exists())
      scala.sys.process.Process("rm -rf " + unsorted).!
    if(sorted.exists())
      scala.sys.process.Process("rm -rf " + sorted).!
  }

}
